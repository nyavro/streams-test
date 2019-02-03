name := "streams-test"

version := "0.1"

scalaVersion := "2.12.6"

// PROJECTS
lazy val global = project
  .in(file("."))
  .settings(settings)
  .aggregate(
    db, gen, rest
  )

lazy val db = project.in(file("db"))
lazy val gen = project.in(file("gen")).dependsOn(db)
lazy val rest = project.in(file("rest")).dependsOn(gen)

lazy val settings = commonSettings

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8"
)

lazy val commonSettings = Seq(
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)
