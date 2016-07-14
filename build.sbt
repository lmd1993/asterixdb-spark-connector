//Specify the versions of both Hyracks and Spark
val hyracksVersion = "0.2.18-SNAPSHOT"
val sparkVersion = "1.6.1"
val scope = "provided"

name := "asterixdb-spark-connector"

version := sparkVersion

scalaVersion := "2.10.5"



resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.hyracks" % "hyracks-api" % hyracksVersion excludeAll ExclusionRule(organization = "org.slf4j")

libraryDependencies += "org.apache.hyracks" % "hyracks-client" % hyracksVersion excludeAll ExclusionRule(organization = "org.slf4j")

libraryDependencies += "org.apache.hyracks" % "hyracks-control-nc" % hyracksVersion excludeAll ExclusionRule(organization = "org.slf4j")

libraryDependencies += "org.apache.hyracks" % "hyracks-dataflow-common" % hyracksVersion excludeAll ExclusionRule(organization = "org.slf4j")

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % scope

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % scope

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % scope

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % scope

libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % scope

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % scope


libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5" excludeAll ExclusionRule(organization = "org.slf4j")

libraryDependencies += "net.liftweb" %% "lift-json" % "2.6.2"

libraryDependencies += "junit" % "junit" % "4.11" % "test"

libraryDependencies += "org.json" % "json" % "20140107" %"provided"

excludeFilter in unmanagedResources := HiddenFileFilter || "*properties" || "*xml"
excludeFilter in unmanagedBase := HiddenFileFilter || "*properties" || "*xml"

val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".RSA" => MergeStrategy.first
  case meta(_) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
