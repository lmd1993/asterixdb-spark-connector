name := "asterix-connector"

version := "1.6.0"

scalaVersion := "2.10.4"

resolvers += Resolver.mavenLocal


libraryDependencies += "org.apache.asterix" % "asterix-app" % "0.8.8-SNAPSHOT" excludeAll(ExclusionRule(organization = "org.slf4j"))

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5"

libraryDependencies += "net.liftweb" %% "lift-json" % "3.0-M1"

libraryDependencies += "junit" % "junit" % "4.11" % "test"

libraryDependencies += "org.json" % "json" % "20140107"


publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))