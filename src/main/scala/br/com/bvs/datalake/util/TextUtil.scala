package br.com.bvs.datalake.util

object TextUtil {

  private val alphaAccentedRegex = "\\s|(?i)(?:(?![×Þß÷þø])[À-ÿ])"

  private val boaVistaSCPC =
    """
      |  ____               __      ___     _           _____  _____ _____   _____
      | |  _ \              \ \    / (_)   | |         / ____|/ ____|  __ \ / ____|
      | | |_) | ___   __ _   \ \  / / _ ___| |_ __ _  | (___ | |    | |__) | |
      | |  _ < / _ \ / _` |   \ \/ / | / __| __/ _` |  \___ \| |    |  ___/| |
      | | |_) | (_) | (_| |    \  /  | \__ \ || (_| |  ____) | |____| |    | |____
      | |____/ \___/ \__,_|     \/   |_|___/\__\__,_| |_____/ \_____|_|     \_____|
      |
      |
      | Datalake Manager v0.1 started ...
    """.stripMargin

  def printWelcome(): Unit = {
    println(boaVistaSCPC)
  }

  def alphaAccentedCleanUp(text: String): String = {
    text.replaceAll(alphaAccentedRegex, "")
  }

}
