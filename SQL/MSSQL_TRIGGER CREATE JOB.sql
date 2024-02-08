USE msdb;

GO

CREATE TRIGGER TRG_JOB_CREATE
ON sysjobs
AFTER INSERT 

AS
BEGIN

       BEGIN TRANSACTION

             MERGE INTO SOURCES.dbo.T_JOB as TARGET
             
             USING msdb.dbo.sysjobs as SOURCE
             ON ( SOURCE.name=TARGET.JOB_NAME )

             WHEN MATCHED
             THEN UPDATE
             SET TARGET.JOB_NAME=SOURCE.name

             WHEN NOT MATCHED BY TARGET
             THEN INSERT

                    (
                    JOB_NAME
                    )

             VALUES

                    (
                    SOURCE.name
                    );

       COMMIT TRANSACTION;

END