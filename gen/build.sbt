libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick-codegen" % "3.2.3"
)

import slick.codegen.SourceCodeGenerator
import slick.{ model => m }

enablePlugins(CodegenPlugin)

sourceGenerators in Compile += slickCodegen

slickCodegenDatabaseUrl := "jdbc:postgresql://localhost:5432/streams-test"

slickCodegenDatabaseUser := "postgres"

slickCodegenDatabasePassword := "postgres"

slickCodegenJdbcDriver := "org.postgresql.Driver"

slickCodegenOutputPackage := "streams.gen"

slickCodegenCodeGenerator := { (model: m.Model) => new SourceCodeGenerator(model) }

slickCodegenExcludedTables in Compile := Seq("flyway_schema_history")

slickCodegenOutputDir := (sourceManaged in Compile).value