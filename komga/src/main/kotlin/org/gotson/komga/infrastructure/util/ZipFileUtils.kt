package org.gotson.komga.infrastructure.util

import org.apache.commons.compress.archivers.zip.ZipFile
import org.gotson.komga.domain.model.EntryNotFoundException
import java.io.InputStream
import java.nio.file.Path

inline fun <R> ZipFile.Builder.use(block: (ZipFile) -> R) = this.get().use(block)

fun ZipFile.getEntryInputStream(entryName: String): InputStream? = this.getEntry(entryName)?.let { entry -> this.getInputStream(entry) }

fun ZipFile.getEntryBytes(entryName: String): ByteArray? = this.getEntry(entryName)?.let { entry -> this.getInputStream(entry).use { it.readBytes() } }

fun getZipEntryBytes(
  path: Path,
  entryName: String,
): ByteArray {
  // fast path. Only read central directory record and try to find entry in it
  val zipBuilder =
    ZipFile
      .builder()
      .setPath(path)
      .setUseUnicodeExtraFields(true)
      .setIgnoreLocalFileHeader(true)
  val bytes = zipBuilder.use { it.getEntryBytesClosing(entryName) }
  if (bytes != null) return bytes

  // slow path. Entry with that name wasn't in central directory record
  // Iterate each entry and, if present, set name from Unicode extra field in local file header
  return zipBuilder.setIgnoreLocalFileHeader(false).use {
    it.getEntryBytesClosing(entryName)
      ?: throw EntryNotFoundException("Entry does not exist: $entryName")
  }
}

private fun ZipFile.getEntryBytesClosing(entryName: String) =
  this.use { zip ->
    zip.getEntry(entryName)?.let { entry ->
      zip.getInputStream(entry).use { it.readBytes() }
    }
  }

/**
 * Extract multiple entries from a ZIP file in a single operation.
 * Opens the archive once and extracts all requested entries.
 *
 * @param path Path to the ZIP file
 * @param entryNames List of entry names to extract
 * @return Map of entry name to bytes for all successfully extracted entries
 * @throws EntryNotFoundException if any requested entry is not found
 */
fun getZipEntriesBatch(
  path: Path,
  entryNames: List<String>,
): Map<String, ByteArray> {
  // fast path. Only read central directory record and try to find entries in it
  val zipBuilder =
    ZipFile
      .builder()
      .setPath(path)
      .setUseUnicodeExtraFields(true)
      .setIgnoreLocalFileHeader(true)

  val fastPathResults = zipBuilder.use { zip ->
    entryNames.mapNotNull { entryName ->
      zip.getEntryBytes(entryName)?.let { bytes -> entryName to bytes }
    }.toMap()
  }

  // If all entries were found, return
  if (fastPathResults.size == entryNames.size) return fastPathResults

  // slow path. Some entries weren't in central directory record
  // Iterate each entry and, if present, set name from Unicode extra field in local file header
  val missingEntries = entryNames.filter { it !in fastPathResults.keys }
  val slowPathResults = zipBuilder.setIgnoreLocalFileHeader(false).use { zip ->
    missingEntries.mapNotNull { entryName ->
      zip.getEntryBytes(entryName)?.let { bytes -> entryName to bytes }
    }.toMap()
  }

  val allResults = fastPathResults + slowPathResults
  val stillMissing = entryNames.filter { it !in allResults.keys }
  if (stillMissing.isNotEmpty()) {
    throw EntryNotFoundException("Entries do not exist: ${stillMissing.joinToString(", ")}")
  }

  return allResults
}
