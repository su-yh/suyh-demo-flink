DROP TABLE IF EXISTS flink_user;

CREATE TABLE flink_user (
    id INTEGER AUTO_INCREMENT COMMENT '主键ID',
    username VARCHAR(256) NULL ,
    age integer NULL ,
    email VARCHAR(256) NULL,
    create_date DATETIME(3) NULL,

    PRIMARY KEY (id)
) ENGINE = INNODB ;
