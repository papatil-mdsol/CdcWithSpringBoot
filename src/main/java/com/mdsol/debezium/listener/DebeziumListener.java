package com.mdsol.debezium.listener;


import com.mdsol.debezium.service.CustomerService;
import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.stereotype.Component;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static io.debezium.data.Envelope.FieldName.OPERATION;
import static io.debezium.data.Envelope.Operation;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Component
public class DebeziumListener {

  private final Executor executor = Executors.newSingleThreadExecutor();
  private final CustomerService customerService;
  private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine1;
  private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine2;

  public DebeziumListener(Configuration customerConnectorConfiguration1,CustomerService customerService) {

    this.debeziumEngine1 = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
      .using(customerConnectorConfiguration1.asProperties())
      .notifying(this::handleChangeEvent)
      .build();

    this.debeziumEngine2 = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
      .using(customerConnectorConfiguration1.asProperties())
      .notifying(this::handleChangeEvent)
      .build();

    this.customerService = customerService;
  }

  private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent) {
    SourceRecord sourceRecord = sourceRecordRecordChangeEvent.record();

    log.info("Key = '" + sourceRecord.key() + "' value = '" + sourceRecord.value() + "'");

    Struct sourceRecordChangeValue= (Struct) sourceRecord.value();

    if (sourceRecordChangeValue != null) {
      Operation operation = Operation.forCode((String) sourceRecordChangeValue.get(OPERATION));

      if(operation != Operation.READ) {
        String dbRecord = operation == Operation.DELETE ? BEFORE : AFTER; // Handling Update & Insert operations.

        Struct struct = (Struct) sourceRecordChangeValue.get(dbRecord);
        Map<String, Object> payload = struct.schema().fields().stream()
          .map(Field::name)
          .filter(fieldName -> struct.get(fieldName) != null)
          .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
          .collect(toMap(Pair::getKey, Pair::getValue));

        this.customerService.replicateData(payload, operation);
        log.info("Updated Data: {} with Operation: {}", payload, operation.name());
      }
    }
  }

  @PostConstruct
  private void start() {
    this.executor.execute(debeziumEngine1);
    this.executor.execute(debeziumEngine2);
  }

  @PreDestroy
  private void stop() throws IOException {
    if (this.debeziumEngine1 != null) {
      this.debeziumEngine1.close();
    }
    if (this.debeziumEngine2 != null) {
      this.debeziumEngine2.close();
    }
  }

}
