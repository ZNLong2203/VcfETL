CREATE TABLE variant(
    id serial PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    chrom VARCHAR(255) NOT NULL,
    pos VARCHAR(255) NOT NULL,
    ref VARCHAR(255) NOT NULL,
    alt VARCHAR(255) NOT NULL,
    qual FLOAT NOT NULL,
    filter VARCHAR(255) NOT NULL,
    info TEXT NOT NULL
);

CREATE TABLE format(
    id serial PRIMARY KEY,
    variant_id INT NOT NULL,
    sample_id INT NOT NULL,
    allelic_depth FLOAT NOT NULL,
    allele_frequency FLOAT NOT NULL,
    genotype VARCHAR(255) NOT NULL
);

CREATE TABLE sample(
    id serial PRIMARY KEY,
    name VARCHAR(255)
);