package is.hail.linalg

import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test

class GridPartitionerSuite extends TestNGSuite {

  private def assertLayout(hg: GridPartitioner, layout: ((Int, Int), Int)*) {
    layout.foreach { case ((i, j), p) =>
      assert(hg.coordinatesBlock(i, j) === p, s"at coordinates ${(i,j)}")
    }
    layout.foreach { case ((i, j), p) =>
      assert(hg.blockCoordinates(p) === (i, j), s"at pid $p")
    }
  }

  @Test
  def squareIsColumnMajor() {
    assertLayout(GridPartitioner(2, 4, 4),
      (0, 0) -> 0,
      (1, 0) -> 1,
      (0, 1) -> 2,
      (1, 1) -> 3
    )
  }

  @Test
  def rectangleMoreRowsIsColumnMajor() {
    assertLayout(GridPartitioner(2, 6, 4),
      (0, 0) -> 0,
      (1, 0) -> 1,
      (2, 0) -> 2,
      (0, 1) -> 3,
      (1, 1) -> 4,
      (2, 1) -> 5
    )
  }

  @Test
  def rectangleMoreColsIsColumnMajor() {
    assertLayout(GridPartitioner(2, 4, 6),
      (0, 0) -> 0,
      (1, 0) -> 1,
      (0, 1) -> 2,
      (1, 1) -> 3,
      (0, 2) -> 4,
      (1, 2) -> 5
    )
  }

  @Test
  def bandedBlocksTest() {
    // 0  3  6  9
    // 1  4  7 10
    // 2  5  8 11
    val gp1 = GridPartitioner(10, 30, 40)
    val gp2 = GridPartitioner(10, 21, 31)

    for (gp <- Seq(gp1, gp2)) {
      assert(gp.bandedBlocks(0, 0) sameElements Array(0, 4, 8))

      assert(gp.bandedBlocks(1, 0) sameElements Array(0, 1, 4, 5, 8))
      assert(gp.bandedBlocks(1, 0) sameElements gp.bandedBlocks(10, 0))

      assert(gp.bandedBlocks(0, 1) sameElements Array(0, 3, 4, 7, 8, 11))
      assert(gp.bandedBlocks(0, 1) sameElements gp.bandedBlocks(0, 10))

      assert(gp.bandedBlocks(1, 1) sameElements Array(0, 1, 3, 4, 5, 7, 8, 11))
      assert(gp.bandedBlocks(1, 1) sameElements gp.bandedBlocks(10, 10))

      assert(gp.bandedBlocks(11, 0) sameElements Array(0, 1, 2, 4, 5, 8))

      assert(gp.bandedBlocks(0, 11) sameElements Array(0, 3, 4, 6, 7, 8, 10, 11))
      assert(gp.bandedBlocks(0, 20) sameElements gp.bandedBlocks(0, 11))
      assert(gp.bandedBlocks(0, 21) sameElements Array(0, 3, 4, 6, 7, 8, 9, 10, 11))

      assert(gp.bandedBlocks(1000, 1000) sameElements (0 until 12))
    }
  }
  
  @Test
  def rectangularBlocksTest() {
    // 0  3  6  9
    // 1  4  7 10
    // 2  5  8 11
    val gp1 = GridPartitioner(10, 30, 40)
    val gp2 = GridPartitioner(10, 21, 31)

    for (gp <- Seq(gp1, gp2)) {
      assert(gp.rectangularBlocks(0, 0, 0, 0) sameElements Array(0))
      assert(gp.rectangularBlocks(Array(Array(0, 0, 0, 0))) sameElements Array(0))

      assert(gp.rectangularBlocks(0, 9, 0, 9) sameElements Array(0))

      assert(gp.rectangularBlocks(9, 10, 9, 10) sameElements Array(0, 1, 3, 4))
      assert(gp.rectangularBlocks(Array(Array(9, 10, 9, 10))) sameElements Array(0, 1, 3, 4))
      
      assert(gp.rectangularBlocks(10, 19, 10, 29) sameElements Array(4, 7))

      assert(gp.rectangularBlocks(Array(
        Array(9, 10, 9, 10), Array(10, 19, 10, 29), Array(0, 0, 20, 20), Array(20, 20, 20, 30)))
        sameElements Array(0, 1, 3, 4, 6, 7, 8, 11))

      assert(gp.rectangularBlocks(0, 20, 0, 30) sameElements (0 until 12))
    }
  }

  @Test
  def triangularBlocksTest() {
    // 0 4 8 12
    // 1 5 9 13
    // 2 6 10 14
    // 3 7 11 15
    val gp = GridPartitioner(blockSize = 10, nRows = 40, nCols = 40)

    val intervals: Array[Array[Long]] = Array(Array(0, 3), Array(4, 9), Array(10, 11),
      Array(12, 20), Array(23, 30), Array(31, 33), Array(35, 39))

    assert(gp.triangularBlocks(12, 20) sameElements Array(5, 9, 10))
    assert(gp.triangularBlocks(intervals) sameElements Array(0, 5, 9, 10, 14, 15))
  }

  @Test
  def triangularBlocksTestWithBlockSizeOfOne() {
    // 0 6  12 18 24 30
    // 1 7  13 19 25 31
    // 2 8  14 20 26 32
    // 3 9  15 21 27 33
    // 4 10 16 22 28 34
    // 5 11 17 23 29 35

    val gp = GridPartitioner(blockSize = 1, nRows = 6, nCols = 6)

    val intervals: Array[Array[Long]] = Array(Array(0, 1), Array(2, 3), Array(4, 5))

    assert(gp.triangularBlocks(intervals) sameElements Array(0, 6, 7, 14, 20, 21, 28, 34, 35))
  }

  @Test
  def testBreakUpOverlappingSquares() {
    val gp = GridPartitioner(blockSize = 1, nRows = 6, nCols = 6)
    val (squares, rectangles) = gp.breakUpOverlappingSquares(0, 1, 1, 2, aboveDiagonalOnly = false)
    assert(squares.deep == Array(Array(0, 0), Array(1, 1), Array(2, 2)).deep)
    assert(rectangles.deep == Array(Array(1, 1, 0, 0), Array(0, 0, 1, 1), Array(2, 2, 1, 1), Array(1, 1, 2, 2)).deep)
  }

  @Test
  def testSquareBlocksAboveAndBelowDiagonal() {
    // 0 6  12 18 24 30
    // 1 7  13 19 25 31
    // 2 8  14 20 26 32
    // 3 9  15 21 27 33
    // 4 10 16 22 28 34
    // 5 11 17 23 29 35

    val gp = GridPartitioner(blockSize = 1, nRows = 6, nCols = 6)

    assert(gp.squareBlocks(Array(Array(0, 1), Array(1, 3), Array(2, 5)), aboveDiagonalOnly = false) sameElements
      Array(0, 1, 6, 7, 8, 9, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23, 26, 27, 28, 29, 32, 33, 34, 35))

    assert(gp.squareBlocks(Array(Array(0, 1), Array(1, 3), Array(2, 5)), aboveDiagonalOnly = true) sameElements
      Array(0, 6, 7, 13, 14, 19, 20, 21, 26, 27, 28, 32, 33, 34, 35))
  }
  
}
