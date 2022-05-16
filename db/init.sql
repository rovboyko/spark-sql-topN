use flink;

create table result (
     `name` VARCHAR(20),
     `lines` BIGINT,
     `cnt` BIGINT,
     `ts` BIGINT,
     `rank` BIGINT PRIMARY KEY
)
ENGINE=InnoDB;