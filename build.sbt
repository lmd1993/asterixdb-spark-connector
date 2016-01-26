name := "asterix-connector"

version := "1.0"

scalaVersion := "2.10.5"

//Specify the versions of both Hyracks and Spark
val hyracksVersion = "0.2.17-SNAPSHOT"
val sparkVersion = "1.6.0"


resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.hyracks" % "hyracks-api" % hyracksVersion

libraryDependencies += "org.apache.hyracks" % "hyracks-client" % hyracksVersion

libraryDependencies += "org.apache.hyracks" % "hyracks-control-nc" % hyracksVersion

libraryDependencies += "org.apache.hyracks" % "hyracks-dataflow-common" % hyracksVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"


libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5"

libraryDependencies += "net.liftweb" %% "lift-json" % "2.6.2"

libraryDependencies += "junit" % "junit" % "4.11" % "test"

libraryDependencies += "org.json" % "json" % "20140107" %"provided"

excludeFilter in unmanagedResources := HiddenFileFilter || "*properties" || "*xml"
excludeFilter in unmanagedBase := HiddenFileFilter || "*properties" || "*xml"

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
