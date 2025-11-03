package org.gotson.komga.domain.service

import io.github.oshai.kotlinlogging.KotlinLogging
import org.gotson.komga.domain.model.BookWithMedia
import org.gotson.komga.domain.model.Media
import org.gotson.komga.domain.model.MediaNotReadyException
import org.gotson.komga.domain.model.MediaProfile
import org.gotson.komga.domain.persistence.BookRepository
import org.gotson.komga.domain.persistence.MediaRepository
import org.gotson.komga.infrastructure.cache.BookPageThumbnailCache
import org.gotson.komga.infrastructure.configuration.KomgaSettingsProvider
import org.gotson.komga.infrastructure.image.ImageConverter
import org.gotson.komga.infrastructure.image.ImageType
import org.gotson.komga.infrastructure.util.getZipEntriesBatch
import org.gotson.komga.domain.model.BookPage
import org.springframework.core.task.AsyncTaskExecutor
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.min

private val logger = KotlinLogging.logger {}

private const val BATCH_SIZE = 50
private const val THUMBNAIL_WAIT_TIMEOUT_SECONDS = 60L
private const val BATCH_GENERATION_TIMEOUT_SECONDS = 300L
private const val FAILURE_COOLDOWN_MS = 5 * 60 * 1000L

fun sanitizeFilename(filename: String): String =
  filename.replace('/', '_').replace('\\', '_')

data class BatchRange(
  val bookId: String,
  val startIndex: Int,
  val endIndex: Int,
)

@Service
class BookPageThumbnailBatchService(
  private val bookRepository: BookRepository,
  private val mediaRepository: MediaRepository,
  private val bookAnalyzer: BookAnalyzer,
  private val imageConverter: ImageConverter,
  private val bookPageThumbnailCache: BookPageThumbnailCache,
  private val komgaSettingsProvider: KomgaSettingsProvider,
  private val applicationTaskExecutor: AsyncTaskExecutor,
) {
  private val resizeTargetFormat = ImageType.JPEG

  private fun getBatchForPage(
    bookId: String,
    filename: String,
    pages: List<BookPage>,
  ): BatchRange {
    val pageIndex = pages.indexOfFirst { sanitizeFilename(it.fileName) == filename }
    if (pageIndex == -1) throw IllegalArgumentException("Page not found: $filename")

    val batchStart = (pageIndex / BATCH_SIZE) * BATCH_SIZE
    val batchEnd = min(batchStart + BATCH_SIZE, pages.size)
    return BatchRange(bookId, batchStart, batchEnd)
  }

  private fun generatePagesAsync(
    bookId: String,
    pages: List<BookPage>,
    media: Media,
  ) {
    logger.debug { "Generating ${pages.size} pages for book $bookId" }

    val book = bookRepository.findByIdOrNull(bookId)
      ?: throw IllegalArgumentException("Book not found: $bookId")
    val bookWithMedia = BookWithMedia(book, media)

    val pageFileNames = pages.map { it.fileName }
    val pageContents = when (media.profile) {
      MediaProfile.DIVINA -> {
        if (media.mediaType?.contains("zip", ignoreCase = true) == true) {
          getZipEntriesBatch(book.path, pageFileNames)
        } else {
          pageFileNames.associateWith { filename ->
            val pageIndex = media.pages.indexOfFirst { it.fileName == filename }
            bookAnalyzer.getPageContent(bookWithMedia, pageIndex + 1)
          }
        }
      }
      MediaProfile.PDF -> {
        pages.associate { page ->
          val pageIndex = media.pages.indexOf(page)
          page.fileName to bookAnalyzer.getPageContent(bookWithMedia, pageIndex + 1)
        }
      }
      MediaProfile.EPUB -> {
        if (!media.epubDivinaCompatible) {
          logger.warn { "EPUB not divina-compatible" }
          return
        }
        getZipEntriesBatch(book.path, pageFileNames)
      }
      null -> throw MediaNotReadyException()
    }

    val normalizedPages = pageContents.mapKeys { (filename, _) -> sanitizeFilename(filename) }
    val thumbnailSize = komgaSettingsProvider.thumbnailSize.maxEdge

    val futures = normalizedPages.map { (filename, bytes) ->
      CompletableFuture.supplyAsync(
        {
          try {
            val thumbnail = imageConverter.resizeImageToByteArray(bytes, resizeTargetFormat, thumbnailSize)
            bookPageThumbnailCache.putThumbnail(bookId, filename, thumbnail)
            logger.debug { "Page cached: $bookId/$filename" }
            filename to thumbnail
          } catch (e: Exception) {
            logger.error(e) { "Failed to resize page: $filename" }
            bookPageThumbnailCache.completeThumbnailExceptionally(bookId, filename, e)
            null
          }
        },
        applicationTaskExecutor,
      )
    }

    try {
      CompletableFuture.allOf(*futures.toTypedArray())
        .orTimeout(BATCH_GENERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .join()

      val successfulThumbnails = futures.mapNotNull { it.get() }.toMap()
      if (successfulThumbnails.isNotEmpty()) {
        bookPageThumbnailCache.putThumbnails(bookId, successfulThumbnails)
      }

      logger.debug { "Completed generation for book $bookId: ${successfulThumbnails.size}/${pages.size} pages succeeded" }
    } catch (e: Exception) {
      logger.error(e) { "Batch generation timed out or failed for book $bookId" }
      throw e
    }
  }

  fun getThumbnail(
    bookId: String,
    filename: String,
  ): ByteArray? {
    val cached = bookPageThumbnailCache.getThumbnail(bookId, filename)
    if (cached != null) return cached

    if (bookPageThumbnailCache.isPageRecentlyFailed(bookId, filename)) {
      logger.debug { "Page $bookId/$filename recently failed, failing fast" }
      return null
    }

    val media = mediaRepository.findById(bookId)
    val batchRange =
      try {
        getBatchForPage(bookId, filename, media.pages)
      } catch (e: IllegalArgumentException) {
        logger.warn { "Page not found: $bookId/$filename" }
        return null
      }

    val batchPages = media.pages.subList(batchRange.startIndex, batchRange.endIndex)
    val candidatePages =
      batchPages.filter { page ->
        val name = sanitizeFilename(page.fileName)
        val pageCached = bookPageThumbnailCache.getThumbnail(bookId, name) != null
        val inFlight = bookPageThumbnailCache.hasFuture(bookId, name)
        val failed = bookPageThumbnailCache.isPageRecentlyFailed(bookId, name)
        !pageCached && !inFlight && !failed
      }

    if (candidatePages.isNotEmpty()) {
      logger.debug { "Batch [${batchRange.startIndex}-${batchRange.endIndex}): ${candidatePages.size} pages need generation" }

      val futuresCreated =
        candidatePages.map { page ->
          val name = sanitizeFilename(page.fileName)
          bookPageThumbnailCache.createFuture(bookId, name)
        }

      if (futuresCreated.any { it }) {
        val pagesToGenerate = candidatePages.filterIndexed { i, _ -> futuresCreated[i] }
        logger.debug { "Starting generation for ${pagesToGenerate.size} pages" }

        applicationTaskExecutor.execute {
          try {
            generatePagesAsync(bookId, pagesToGenerate, media)
          } catch (e: Exception) {
            logger.error(e) { "Failed to generate pages for book $bookId" }

            val isTimeout = e is TimeoutException ||
                           (e is java.util.concurrent.CompletionException && e.cause is TimeoutException)

            pagesToGenerate.forEach { page ->
              if (isTimeout) {
                bookPageThumbnailCache.completeThumbnailExceptionallyWithoutCaching(
                  bookId,
                  sanitizeFilename(page.fileName),
                  e,
                )
              } else {
                bookPageThumbnailCache.completeThumbnailExceptionally(
                  bookId,
                  sanitizeFilename(page.fileName),
                  e,
                )
              }
            }
          }
        }
      }
    }

    val future = bookPageThumbnailCache.waitForThumbnail(bookId, filename)
    return if (future != null) {
      try {
        future.get(THUMBNAIL_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
      } catch (e: TimeoutException) {
        logger.warn { "Timeout waiting for thumbnail: $bookId/$filename" }
        null
      } catch (e: ExecutionException) {
        logger.error(e.cause) { "Failed to generate thumbnail: $bookId/$filename" }
        null
      } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        logger.warn { "Interrupted while waiting for thumbnail: $bookId/$filename" }
        null
      }
    } else {
      bookPageThumbnailCache.getThumbnail(bookId, filename)
    }
  }
}
