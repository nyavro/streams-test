enablePlugins(FlywayPlugin)

flywayUrl := "jdbc:postgresql://localhost:5432/streams-test?createDatabaseIfNotExist=true"
flywayUser := "postgres"
flywayPassword := "postgres"
flywayLocations := Seq("filesystem:db/src/main/resources/db/migration")