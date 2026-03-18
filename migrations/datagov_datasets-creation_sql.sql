CREATE SCHEMA IF NOT EXISTS datagov_datasets;
USE datagov_datasets;

CREATE TABLE IF NOT EXISTS Publisher (
    Id VARCHAR(255),
    Name VARCHAR(255) NOT NULL,
    Title VARCHAR(255),
    Description TEXT,
    Type VARCHAR(255),
    Email VARCHAR(1024),
    Phone VARCHAR(1024),
    WebsiteURL VARCHAR(2048),

    CONSTRAINT pk_publisher
        PRIMARY KEY (ID)
);

CREATE TABLE IF NOT EXISTS Dataset (
    Id VARCHAR(255),
    Name VARCHAR(255) NOT NULL,
    Description TEXT,
    License VARCHAR(255),
    CreationDate DATE,
    UpdateDate DATE,
    Maintainer VARCHAR(255),
    AccessLevel ENUM('public','restricted public','non-public'),
    PublisherId VARCHAR(255),

    CONSTRAINT pk_dataset
        PRIMARY KEY (Id),

    CONSTRAINT fk_dataset_publisher
        FOREIGN KEY (PublisherId)
        REFERENCES Publisher(Id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS DatasetTopic (
    DatasetId VARCHAR(255),
    Topic VARCHAR(255),
    CONSTRAINT pk_datasettopic PRIMARY KEY (DatasetId, Topic),
    CONSTRAINT fk_topic_dataset
        FOREIGN KEY (DatasetId)
        REFERENCES Dataset(Id)
);

CREATE TABLE IF NOT EXISTS DatasetTag (
    DatasetId VARCHAR(255),
    Tag VARCHAR(255),
    CONSTRAINT pk_datasettag PRIMARY KEY (DatasetId, Tag),
    CONSTRAINT fk_tag_dataset
        FOREIGN KEY (DatasetId)
        REFERENCES Dataset(Id)
);

CREATE TABLE IF NOT EXISTS Resource (
    DatasetId VARCHAR(255),
    URLHash BINARY(32),
    URL VARCHAR(2048),
    Name VARCHAR(1024),
    Format VARCHAR(100),
    Description TEXT,
    CONSTRAINT pk_resource PRIMARY KEY (DatasetId, URLHash),
    CONSTRAINT fk_resource_dataset
        FOREIGN KEY (DatasetId)
        REFERENCES Dataset(Id)
);

CREATE TABLE IF NOT EXISTS AppUser (
    Email VARCHAR(255),
    Username VARCHAR(255),
    Gender VARCHAR(100),
    Birthdate DATE,
    Country VARCHAR(100),
    CONSTRAINT pk_appuser PRIMARY KEY (Email)
);

CREATE TABLE IF NOT EXISTS Project (
    AppUserEmail VARCHAR(255),
    Name VARCHAR(255),
    ProjectCategory ENUM('analytics','machine learning','field research'),
    CONSTRAINT pk_project PRIMARY KEY (AppUserEmail, Name),
    CONSTRAINT fk_project_user
        FOREIGN KEY (AppUserEmail)
        REFERENCES AppUser(Email)
);

CREATE TABLE IF NOT EXISTS ProjectDatasets (
    AppUserEmail VARCHAR(255),
    ProjectName VARCHAR(255),
    DatasetId VARCHAR(255),
    CONSTRAINT pk_projectdatasets PRIMARY KEY (AppUserEmail, ProjectName, DatasetId),
    CONSTRAINT fk_pd_project
        FOREIGN KEY (AppUserEmail, ProjectName)
        REFERENCES Project(AppUserEmail, Name),
    CONSTRAINT fk_pd_dataset
        FOREIGN KEY (DatasetId)
        REFERENCES Dataset(Id)
);
