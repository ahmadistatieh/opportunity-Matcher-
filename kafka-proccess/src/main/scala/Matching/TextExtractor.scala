package Matching

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.poi.hwpf.HWPFDocument
import org.apache.poi.hwpf.extractor.WordExtractor
import org.apache.poi.xwpf.extractor.XWPFWordExtractor
import org.apache.poi.xwpf.usermodel.XWPFDocument

object TextExtractor {

  def extractText(path: Path): String = {
    val name = path.getFileName.toString.toLowerCase

    val raw: String =
      if (name.endsWith(".txt")) {
        new String(Files.readAllBytes(path), StandardCharsets.UTF_8)

      } else if (name.endsWith(".pdf")) {
        val doc = PDDocument.load(path.toFile)
        try new PDFTextStripper().getText(doc)
        finally doc.close()

      } else if (name.endsWith(".docx")) {
        val is = Files.newInputStream(path)
        try {
          val docx = new XWPFDocument(is)
          val ex = new XWPFWordExtractor(docx)
          try ex.getText
          finally ex.close()
        } finally is.close()

      } else if (name.endsWith(".doc")) {
        val is = Files.newInputStream(path)
        try {
          val doc = new HWPFDocument(is)
          val ex = new WordExtractor(doc)
          try ex.getText
          finally ex.close()
        } finally is.close()

      } else {
        ""
      }

    raw
      .replace("\r\n", "\n")
      .replace("\r", "\n")
      .trim
  }
}
