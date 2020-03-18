DROP TABLE IF EXISTS billionaires;

CREATE TABLE billionaires (
  id VARCHAR(250)   PRIMARY KEY,
  first_name VARCHAR(250) NOT NULL,
  UNIQUE KEY cons (id)
);

INSERT INTO billionaires (id, first_name) VALUES
('1' , 'Aliko' ),
('2', 'Bill'),
('3', 'Folrunsho');