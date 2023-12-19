CREATE TABLE year_dim (
    Year_Id INT PRIMARY KEY,
    Year INT
);

CREATE TABLE country_dim (
    Country_Id INT PRIMARY KEY,
    Country VARCHAR
);

CREATE TABLE region_dim (
    WHO_Region_Id INT PRIMARY KEY,
    WHO_Region VARCHAR
);

CREATE TABLE income_dim (
    WB_Income_Group_Id INT PRIMARY KEY,
    WB_Income_Group VARCHAR
);


CREATE TABLE neonatal_dim (
    NMR_Id INT PRIMARY KEY,
    Indicator VARCHAR,
    Year_Id INT,
    Country_Id INT,
    WHO_Region_Id INT,
    WB_Income_Group_Id INT,
    Value INT,
    Value_Low INT,
    Value_High INT,
    FOREIGN KEY (Year_Id) REFERENCES year_dim(Year_Id),
    FOREIGN KEY (Country_Id) REFERENCES country_dim(Country_Id),
    FOREIGN KEY (WHO_Region_Id) REFERENCES region_dim(WHO_Region_Id),
    FOREIGN KEY (WB_Income_Group_Id) REFERENCES income_dim(WB_Income_Group_Id)
);

CREATE TABLE maternal_mortality_dim (
    MMR_Id INT PRIMARY KEY,
    Indicator VARCHAR,
    Year_Id INT,
    Country_Id INT,
    WHO_Region_Id INT,
    WB_Income_Group_Id INT,
    Value INT,
    Value_Low INT,
    Value_High INT,
    FOREIGN KEY (Year_Id) REFERENCES year_dim(Year_Id),
    FOREIGN KEY (Country_Id) REFERENCES country_dim(Country_Id),
    FOREIGN KEY (WHO_Region_Id) REFERENCES region_dim(WHO_Region_Id),
    FOREIGN KEY (WB_Income_Group_Id) REFERENCES income_dim(WB_Income_Group_Id)
);

CREATE TABLE antenatal_dim (
    ACC_Id INT PRIMARY KEY,
    Indicator VARCHAR,
    Year_Id INT,
    Country_Id INT,
    WHO_Region_Id INT,
    WB_Income_Group_Id INT,
    Value INT,
    FOREIGN KEY (Year_Id) REFERENCES year_dim(Year_Id),
    FOREIGN KEY (Country_Id) REFERENCES country_dim(Country_Id),
    FOREIGN KEY (WHO_Region_Id) REFERENCES region_dim(WHO_Region_Id),
    FOREIGN KEY (WB_Income_Group_Id) REFERENCES income_dim(WB_Income_Group_Id)
);

CREATE TABLE skilled_health_dim (
    SHP_Id INT PRIMARY KEY,
    Indicator VARCHAR,
    Year_Id INT,
    Country_Id INT,
    WHO_Region_Id INT,
    WB_Income_Group_Id INT,
    Value INT,
    FOREIGN KEY (Year_Id) REFERENCES year_dim(Year_Id),
    FOREIGN KEY (Country_Id) REFERENCES country_dim(Country_Id),
    FOREIGN KEY (WHO_Region_Id) REFERENCES region_dim(WHO_Region_Id),
    FOREIGN KEY (WB_Income_Group_Id) REFERENCES income_dim(WB_Income_Group_Id)
);


CREATE TABLE healthcare_disparities_fct (
    Health_Id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    SHP_Id INT,
    MMR_Id INT,
    NMR_Id INT,
    ACC_Id INT,
    Year_Id INT,
    Country_Id INT,
    WHO_Region_Id INT,
    WB_Income_Group_Id INT,
    FOREIGN KEY (SHP_Id) REFERENCES skilled_health_dim(SHP_Id),
    FOREIGN KEY (MMR_Id) REFERENCES maternal_mortality_dim(MMR_Id),
    FOREIGN KEY (NMR_Id) REFERENCES neonatal_dim(NMR_Id),
    FOREIGN KEY (ACC_Id) REFERENCES antenatal_dim(ACC_Id),
    FOREIGN KEY (Year_Id) REFERENCES year_dim(Year_Id),
    FOREIGN KEY (Country_Id) REFERENCES country_dim(Country_Id),
    FOREIGN KEY (WHO_Region_Id) REFERENCES region_dim(WHO_Region_Id),
    FOREIGN KEY (WB_Income_Group_Id) REFERENCES income_dim(WB_Income_Group_Id)
);
