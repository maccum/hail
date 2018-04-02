package is.hail.linalg

import org.apache.spark.Partitioner
import breeze.linalg.{DenseVector => BDV}
import is.hail.utils.fatal


case class GridPartitioner(blockSize: Int, nRows: Long, nCols: Long) extends Partitioner {
  require(nRows > 0 && nRows <= Int.MaxValue.toLong * blockSize)
  require(nCols > 0 && nCols <= Int.MaxValue.toLong * blockSize)
  
  def blockIndex(index: Long): Int = (index / blockSize).toInt

  def blockOffset(index: Long): Int = (index % blockSize).toInt

  val nBlockRows: Int = blockIndex(nRows - 1) + 1
  val nBlockCols: Int = blockIndex(nCols - 1) + 1
  
  val lastBlockRowNRows: Int = blockOffset(nRows - 1) + 1
  val lastBlockColNCols: Int = blockOffset(nCols - 1) + 1
  
  def blockRowNRows(i: Int): Int = if (i < nBlockRows - 1) blockSize else lastBlockRowNRows
  def blockColNCols(j: Int): Int = if (j < nBlockCols - 1) blockSize else lastBlockColNCols

  def blockBlockRow(pi: Int): Int = pi % nBlockRows
  def blockBlockCol(pi: Int): Int = pi / nBlockRows

  def blockDims(pi: Int): (Int, Int) = (blockRowNRows(blockBlockRow(pi)), blockColNCols(blockBlockCol(pi)))
  
  def blockCoordinates(pi: Int): (Int, Int) = (blockBlockRow(pi), blockBlockCol(pi))

  def coordinatesBlock(i: Int, j: Int): Int = {
    require(0 <= i && i < nBlockRows, s"Block row $i out of range [0, $nBlockRows).")
    require(0 <= j && j < nBlockCols, s"Block column $j out of range [0, $nBlockCols).")
    i + j * nBlockRows
  }

  override val numPartitions: Int = nBlockRows * nBlockCols
  
  override def getPartition(key: Any): Int = key match {
    case (i: Int, j: Int) => coordinatesBlock(i, j)
  }
  
  def transpose: GridPartitioner = GridPartitioner(this.blockSize, this.nCols, this.nRows)

  def vectorOnBlockRow(v: BDV[Double], i: Int): BDV[Double] = {
    val firstRow = i * blockSize
    v(firstRow until firstRow + blockRowNRows(i))
  }
  
  def vectorOnBlockCol(v: BDV[Double], j: Int): BDV[Double] = {
    val firstCol = j * blockSize
    v(firstCol until firstCol + blockColNCols(j))
  }
  
  // returns increasing array of all blocks intersecting the diagonal band consisting of
  //   all entries with -lowerBandwidth <= (colIndex - rowIndex) <= upperBandwidth
  def bandedBlocks(lowerBandwidth: Long, upperBandwidth: Long): Array[Int] = {
    require(lowerBandwidth >= 0 && upperBandwidth >= 0)
    
    val lowerBlockBandwidth = blockIndex(lowerBandwidth + blockSize - 1)
    val upperBlockBandwidth = blockIndex(upperBandwidth + blockSize - 1)

    (for { j <- 0 until nBlockCols
           i <- ((j - upperBlockBandwidth) max 0) to
                ((j + lowerBlockBandwidth) min (nBlockRows - 1))
    } yield (j * nBlockRows) + i ).toArray
  }

  // returns increasing array of all blocks intersecting the rectangle [firstRow, lastRow] x [firstCol, lastCol]
  def rectangularBlocks(firstRow: Long, lastRow: Long, firstCol: Long, lastCol: Long): Array[Int] = {
    require(firstRow >= 0 && firstRow <= lastRow && lastRow <= nRows)
    require(firstCol >= 0 && firstCol <= lastCol && lastCol <= nCols)
    
    val firstBlockRow = blockIndex(firstRow)
    val lastBlockRow = blockIndex(lastRow)
    val firstBlockCol = blockIndex(firstCol)
    val lastBlockCol = blockIndex(lastCol)
    
    (for { j <- firstBlockCol to lastBlockCol
           i <- firstBlockRow to lastBlockRow
    } yield (j * nBlockRows) + i ).toArray
  }

  // returns increasing array of all blocks intersecting the union of rectangles
  def rectangularBlocks(rectangles: Array[Array[Long]]): Array[Int] = {
    require(rectangles.forall(r => r.length == 4))
    val rects = rectangles.foldLeft(Set[Int]())((s, r) => s ++ rectangularBlocks(r(0), r(1), r(2), r(3))).toArray    
    scala.util.Sorting.quickSort(rects)
    rects
  }

  // returns array of blocks intersecting the upper triangular portion of the square defined by the given columns
  def triangularBlocks(firstColumn: Long, lastColumn: Long): Array[Int] = {
    if (nRows != nCols) {
      fatal(s"Expected square block matrix, but found block matrix with $nRows rows and $nCols columns.")
    }

    require(firstColumn <= lastColumn && firstColumn >=0 && lastColumn<=nCols)

    val lo = blockIndex(firstColumn)
    val hi = blockIndex(lastColumn)

    val upperBlockBandwidth = hi - lo

    (for {j <- lo to hi
    i <- ((j - upperBlockBandwidth) max lo) to (j min hi)
    } yield (j * nBlockRows) + i).toArray
  }

  // returns array of all blocks intersecting the upper triangles of a square matrix
  def triangularBlocks(triangles: Array[Array[Long]]): Array[Int] = {
    if (nRows != nCols) {
      fatal(s"Expected square block matrix, but found block matrix with $nRows rows and $nCols columns.")
    }
    require(triangles.forall(t => t.length == 2 && t(0) <= t(1)))
    val tris = triangles.foldLeft(Set[Int]()) { (s, t) => s ++ triangularBlocks(t(0), t(1)) }.toArray
    scala.util.Sorting.quickSort(tris)
    tris
  }

  def squareBlocks(squares: Array[Array[Long]], aboveDiagonalOnly: Boolean): Array[Int] = {
    if (nRows != nCols) {
      fatal(s"Expected square block matrix, but found block matrix with $nRows rows and $nCols columns.")
    }
    // overlapping squares should be separated into non-overlapping squares and rectangles
    require(squares.forall(sq => sq.length==2))
    var rectangles = Array[Array[Long]]()
    val sqs = squares.foldLeft(Array[Array[Long]]()) {
      (s, el) => {
        if (s.isEmpty) {
          s ++ Array(el)
        } else {
          val previous = s.head
          if (el(1) <= previous(1)) {
            // intervals overlap
            val (sqs, rects) = breakUpOverlappingSquares(previous(0), previous(1), el(0), el(1), aboveDiagonalOnly)
            require(rects.forall(r => r.length == 4) && sqs.forall(s => s.length == 2))
            rectangles = rectangles ++ rects
            s.tail ++ sqs
          } else {
            s ++ Array(el)
          }
        }
      }
    }

    if (aboveDiagonalOnly) {
      triangularBlocks(sqs) ++ rectangularBlocks(rectangles)
    } else {
      rectangularBlocks(sqs.map(sq => Array(sq(0), sq(1), sq(0), sq(1)))) ++ rectangularBlocks(rectangles)
    }
  }

  /*Break overlapping squares (a,b) and (c,d) into squares (a,c-1), (c,b), (b+1,d) and
  rectangles (c, b, a, c-1), (a, c-1, c, b), (b+1, d, c, b), (c, b, b+1, d) */
  def breakUpOverlappingSquares(a: Long, b: Long, c: Long, d: Long, aboveDiagonalOnly: Boolean):
  (Array[Array[Long]], Array[Array[Long]]) = {

    require(c <= b) // squares must overlap

    val (squareA, squareB, squareC) = (Array(a, c-1), Array(c, b), Array(b+1, d))

    val squares = Array(squareA, squareB, squareC)
    val rectangles =
      if (aboveDiagonalOnly) {
        Array(squareA ++ squareB, squareB ++ squareC)
      } else {
        Array(squareB ++ squareA, squareA ++ squareB, squareC ++ squareB, squareB ++ squareC)
      }

    (squares, rectangles)
  }
}
