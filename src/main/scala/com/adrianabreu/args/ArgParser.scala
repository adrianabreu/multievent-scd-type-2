package com.adrianabreu.scdtype2delta.args

object ArgParser {
  def parse(args: Array[String]): Map[String, Any] = {
    def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
      list match {
        case Nil => map
        case "-e" :: value :: tail =>
          nextArg(map ++ Map("env" -> value), tail)
        case string :: Nil =>
          nextArg(map ++ Map("filename" -> string), list.tail)
        case unknown :: _ =>
          println("Unknown option " + unknown)
          throw new IllegalArgumentException("invalid option")
      }
    }
    nextArg(Map(), args.toList)
  }
}
