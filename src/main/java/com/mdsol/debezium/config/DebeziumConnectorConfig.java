package com.mdsol.debezium.config;


import java.io.File;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class DebeziumConnectorConfig {

  /**
   * Database details.
   */
  @Value("${customer.datasource1.host}")
  private String customerDbHost1;

  @Value("${customer.datasource1.database}")
  private String customerDbName1;

  @Value("${customer.datasource1.port}")
  private String customerDbPort1;

  @Value("${customer.datasource1.username}")
  private String customerDbUsername1;

  @Value("${customer.datasource1.password}")
  private String customerDbPassword1;

  @Value("${customer.datasource2.host}")
  private String customerDbHost2;

  @Value("${customer.datasource2.database}")
  private String customerDbName2;

  @Value("${customer.datasource2.port}")
  private String customerDbPort2;

  @Value("${customer.datasource2.username}")
  private String customerDbUsername2;

  @Value("${customer.datasource2.password}")
  private String customerDbPassword2;

  /**
   * Customer Database Connector Configuration
   */
  @Bean
  @Primary
  public io.debezium.config.Configuration customerConnector1() throws IOException {
    File offsetStorageTempFile = File.createTempFile("offsets1_", ".dat");
    File dbHistoryTempFile = File.createTempFile("dbhistory1_", ".dat");
    File schemaHistoryFile = File.createTempFile("schemahistory1_", ".dat");
    return io.debezium.config.Configuration.create()
      .with("name", "customer-mysql-connector")
      .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
      .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
      .with("offset.storage.file.filename", offsetStorageTempFile.getAbsolutePath())
      .with("offset.flush.interval.ms", "60000")
      .with("database.hostname", customerDbHost1)
      .with("database.port", customerDbPort1)
      .with("database.user", customerDbUsername1)
      .with("database.password", customerDbPassword1)
      .with("database.dbname", customerDbName1)
      .with("database.include.list", customerDbName1)
      .with("include.schema.changes", "false")
      .with("database.allowPublicKeyRetrieval", "true")
      .with("database.server.id", "10181")
      .with("topic.prefix", "customerdb-connector1")
      .with("database.server.name", "customer-mysql-db-server1")
      .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
      .with("database.history.file.filename", dbHistoryTempFile.getAbsolutePath())
      .with("schema.history.internal",
        "io.debezium.storage.file.history.FileSchemaHistory")
      .with("schema.history.internal.file.filename",
        schemaHistoryFile.getAbsolutePath())
      .build();
  }

  @Bean
  public io.debezium.config.Configuration customerConnector2() throws IOException {
    File offsetStorageTempFile = File.createTempFile("offsets2_", ".dat");
    File dbHistoryTempFile = File.createTempFile("dbhistory2_", ".dat");
    File schemaHistoryFile = File.createTempFile("schemahistory2_", ".dat");
    return io.debezium.config.Configuration.create()
      .with("name", "customer-mysql-connector")
      .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
      .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
      .with("offset.storage.file.filename", offsetStorageTempFile.getAbsolutePath())
      .with("offset.flush.interval.ms", "60000")
      .with("database.hostname", customerDbHost2)
      .with("database.port", customerDbPort2)
      .with("database.user", customerDbUsername2)
      .with("database.password", customerDbPassword2)
      .with("database.dbname", customerDbName2)
      .with("database.include.list", customerDbName2)
      .with("include.schema.changes", "false")
      .with("database.allowPublicKeyRetrieval", "true")
      .with("database.server.id", "10181")
      .with("topic.prefix", "customerdb-connector2")
      .with("database.server.name", "customer-mysql-db-server2")
      .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
      .with("database.history.file.filename", dbHistoryTempFile.getAbsolutePath())
      .with("schema.history.internal",
        "io.debezium.storage.file.history.FileSchemaHistory")
      .with("schema.history.internal.file.filename",
        schemaHistoryFile.getAbsolutePath())
      .build();
  }
}
