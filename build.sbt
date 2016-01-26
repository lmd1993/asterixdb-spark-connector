name := "asterix-connector"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += Resolver.mavenLocal

libraryDependencies := {
  CrossVersion.partialVersion(scalaVersion.value) match {
    // if scala 2.11+ is used, quasiquotes are merged into scala-reflect
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      libraryDependencies.value
    // in Scala 2.10, quasiquotes are provided by macro paradise
    case Some((2, 10)) =>
      libraryDependencies.value ++ Seq(
        compilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full),
        "org.scalamacros" %% "quasiquotes" % "2.0.0" cross CrossVersion.binary)
  }
}

libraryDependencies += "org.apache.asterix" % "asterix-app" % "0.8.8-SNAPSHOT" excludeAll(ExclusionRule(organization = "org.slf4j"))

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5"

libraryDependencies += "net.liftweb" %% "lift-json" % "3.0-M1"

libraryDependencies += "junit" % "junit" % "4.11" % "test"

libraryDependencies += "org.json" % "json" % "20140107"


publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))