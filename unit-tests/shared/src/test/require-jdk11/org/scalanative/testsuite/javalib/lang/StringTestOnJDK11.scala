// Ported from Scala.js, commit: c8ddba0 dated: 2021-12-04
// For Scala Native additions & changes, check GitHub history

package org.scalanative.testsuite.javalib.lang

import org.junit.Test
import org.junit.Assert._

import org.scalanative.testsuite.utils.AssertThrows.assertThrows

import java.{util => ju}
import java.util.{stream => jus}

class StringTestOnJDK11 {
  @Test def repeat(): Unit = {
    assertThrows(classOf[IllegalArgumentException], "".repeat(-1))
    assertTrue("".repeat(0) == "")
    assertTrue("".repeat(1) == "")
    assertTrue("".repeat(100) == "")

    val str = "a_"
    assertThrows(classOf[IllegalArgumentException], str.repeat(-1))
    assertTrue(str.repeat(0) == "")
    assertTrue(str.repeat(1) == "a_")
    assertTrue(str.repeat(3) == "a_a_a_")
    assertTrue(str.repeat(10) == List.fill(10)(str).mkString(""))
    assertTrue(str.repeat(100) == List.fill(100)(str).mkString(""))
    assertTrue(str.repeat(1000) == List.fill(1000)(str).mkString(""))
  }

  @Test def strip(): Unit = {
    assertEquals("", "".strip())
    assertEquals("", " ".strip())
    assertEquals("", "  ".strip())
    assertEquals("", "   ".strip())
    assertEquals("", (" " * 1000).strip())
    assertEquals("\u0394", "\u0394".strip())
    assertEquals("a", "a ".strip())
    assertEquals("a", " a".strip())
    assertEquals("a", " a ".strip())
    assertEquals("a", "  a ".strip())
    assertEquals("a", " a  ".strip())
    assertEquals("a", "  a  ".strip())
    assertEquals("a b", " a b ".strip())
    assertEquals("a  b", " a  b ".strip())
    assertEquals("a_", "a_".strip())
    assertEquals("a_", " a_".strip())
    assertEquals("a_", " a_ ".strip())
    assertEquals("a_", " a_ ".strip())

    assertEquals("A", "\u2028 A \u2028".strip())
    assertEquals("A", "\u2029 A \u2029".strip())
    assertEquals("A", "\u2004 A \u2004".strip())
    assertEquals("A", "\u200A A \u200A".strip())
    assertEquals("A", "\u3000 A \u3000".strip())
    assertEquals("A", "\u200A \u3000 A \u2028 \u2029 \u2004 ".strip())
  }

  @Test def stripLeading(): Unit = {
    assertEquals("", "".stripLeading())
    assertEquals("", " ".stripLeading())
    assertEquals("", "  ".stripLeading())
    assertEquals("", "   ".stripLeading())
    assertEquals("", (" " * 1000).stripLeading())
    assertEquals("\u0394", "\u0394".stripLeading())
    assertEquals("a ", "a ".stripLeading())
    assertEquals("a", " a".stripLeading())
    assertEquals("a ", " a ".stripLeading())
    assertEquals("a ", "  a ".stripLeading())
    assertEquals("a  ", " a  ".stripLeading())
    assertEquals("a  ", "  a  ".stripLeading())
    assertEquals("a b ", " a b ".stripLeading())
    assertEquals("a  b ", " a  b ".stripLeading())
    assertEquals("a_", "a_".stripLeading())
    assertEquals("a_", " a_".stripLeading())
    assertEquals("a_ ", " a_ ".stripLeading())
    assertEquals("a_ ", " a_ ".stripLeading())
    assertEquals("A", " \t\n\r\f\u001C\u001D\u001E\u001FA".stripLeading())

    assertEquals("A ", "\u2028 A ".stripLeading())
    assertEquals("A ", "\u2029 A ".stripLeading())
    assertEquals("A ", "\u2004 A ".stripLeading())
    assertEquals("A ", "\u200A A ".stripLeading())
    assertEquals("A ", "\u3000 A ".stripLeading())
    assertEquals("A ", "\u2028 \u2029 \u2004 \u200A \u3000 A ".stripLeading())
  }

  @Test def stripTrailing(): Unit = {
    assertEquals("", "".stripTrailing())
    assertEquals("", " ".stripTrailing())
    assertEquals("", "  ".stripTrailing())
    assertEquals("", "   ".stripTrailing())
    assertEquals("", (" " * 1000).stripTrailing())
    assertEquals("\u0394", "\u0394".stripTrailing())
    assertEquals("a", "a ".stripTrailing())
    assertEquals(" a", " a".stripTrailing())
    assertEquals(" a", " a ".stripTrailing())
    assertEquals("  a", "  a ".stripTrailing())
    assertEquals(" a", " a  ".stripTrailing())
    assertEquals("  a", "  a  ".stripTrailing())
    assertEquals(" a b", " a b ".stripTrailing())
    assertEquals(" a  b", " a  b ".stripTrailing())
    assertEquals("a_", "a_".stripTrailing())
    assertEquals(" a_", " a_".stripTrailing())
    assertEquals(" a_", " a_ ".stripTrailing())
    assertEquals(" a_", " a_ ".stripTrailing())
    assertEquals("A", "A \t\n\r\f\u001C\u001D\u001E\u001F".stripTrailing())

    assertEquals(" A", " A \u2028".stripTrailing())
    assertEquals(" A", " A \u2029".stripTrailing())
    assertEquals(" A", " A \u2004".stripTrailing())
    assertEquals(" A", " A \u200A".stripTrailing())
    assertEquals(" A", " A \u3000".stripTrailing())
    assertEquals(" A", " A \u2028 \u2029 \u2004 \u200A \u3000".stripTrailing())
  }

  @Test def isBlank(): Unit = {
    assertFalse("a".isBlank())
    assertFalse(" a".isBlank())
    assertFalse("\u00A0".isBlank())
    assertFalse("\u2007".isBlank())
    assertFalse("\u202F".isBlank())

    // from unicode: "Separator: Space, Line, Paragraph"
    assertTrue("\u2028".isBlank())
    assertTrue("\u2029".isBlank())
    assertTrue("\u2004".isBlank())
    assertTrue("\u200A".isBlank())
    assertTrue("\u3000".isBlank())
    assertTrue("\u2028 \u2029 \u2004 \u200A \u3000".isBlank())

    assertTrue("\t".isBlank())
    assertTrue("\n".isBlank())
    assertTrue("\u000B".isBlank())
    assertTrue("\f".isBlank())
    assertTrue("\r".isBlank())
    assertTrue("\u001C".isBlank())
    assertTrue("\u001D".isBlank())
    assertTrue("\u001E".isBlank())
    assertTrue("\u001F".isBlank())
    assertTrue("".isBlank())
    assertTrue(" ".isBlank())
    assertTrue("  ".isBlank())
    assertTrue(" \t\n\r\f\u001C\u001D\u001E\u001F".isBlank())
    assertTrue((" " * 1000).isBlank())
  }

  @Test def lines(): Unit = {
    /* Emma Lazarus  - The New Colossus
     * This version is in the public domain.
     * URL: https://www.nps.gov/stli/learn/historyculture/colossus.htm
     */

    val poemByLine = ju.List.of(
      "",
      "\"Keep, ancient lands, your storied pomp!\" cries she",
      "With silent lips. \"Give me your tired, your poor,",
      "Your huddled masses yearning to breathe free,",
      "The wretched refuse of your teeming shore.",
      "Send these, the homeless, tempest-tost to me,",
      "I lift my lamp beside the golden door!\"",
      ""
    )

    ju.List
      .of(
        ("\n", "LF"),
        ("\r", "CR"),
        ("\r\n", "CRLF")
      )
      .forEach { (pair) =>
        {
          val separator = pair._1
          val ident = pair._2

          val poem = poemByLine
            .stream()
            .collect(jus.Collectors.joining(separator, "", separator))

          val nExpectedLines = poemByLine.size
          assertEquals(s"count ${ident}", nExpectedLines, poem.lines().count())

          val poemAfter = poem
            .lines()
            .collect(jus.Collectors.joining(separator, "", separator))

          assertEquals(s"contents ${ident}", poem, poemAfter)
        }
      }
  }

}
