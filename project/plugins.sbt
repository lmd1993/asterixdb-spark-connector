logLevel := Level.Debug
//resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")