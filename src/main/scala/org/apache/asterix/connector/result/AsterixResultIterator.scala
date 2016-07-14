package org.apache.asterix.connector.result

class AsterixResultIterator[String] (resultReader: AsterixResultReader) extends Iterator[String] {

  private[this] val resultUtils = new ResultUtils(resultReader)

  override def next() = resultUtils.getResultTuple.asInstanceOf[String]

  override def hasNext: Boolean = resultUtils.hasNext
}
