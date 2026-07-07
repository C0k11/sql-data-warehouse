/*
===============================================================================
Proc: etl.run_quality_checks — execute all enabled checks, record, gate
===============================================================================
Runs every enabled check for @layer (or all layers when NULL), writes one row
per check into etl.quality_result, prints a summary, and THROWs if any
error-severity check found violations — so the orchestrator / CI fails.

Usage:
    EXEC etl.run_quality_checks;                 -- all layers
    EXEC etl.run_quality_checks @layer = 'silver';
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE etl.run_quality_checks
    @layer NVARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @check_id INT, @check_name NVARCHAR(200), @severity NVARCHAR(10),
            @check_sql NVARCHAR(MAX), @violations INT,
            @failed INT = 0, @warned INT = 0, @passed INT = 0;

    DECLARE check_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT check_id, check_name, severity, check_sql
        FROM etl.quality_check
        WHERE is_enabled = 1 AND (@layer IS NULL OR layer = @layer)
        ORDER BY check_id;

    OPEN check_cursor;
    FETCH NEXT FROM check_cursor INTO @check_id, @check_name, @severity, @check_sql;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @sql NVARCHAR(MAX) =
            N'SELECT @c = COUNT(*) FROM (' + @check_sql + N') AS q;';
        EXEC sp_executesql @sql, N'@c INT OUTPUT', @c = @violations OUTPUT;

        DECLARE @status NVARCHAR(10) =
            CASE WHEN @violations = 0 THEN 'pass'
                 WHEN @severity = 'warn' THEN 'warn'
                 ELSE 'fail'
            END;
        INSERT INTO etl.quality_result (check_id, violations, status)
        VALUES (@check_id, @violations, @status);

        IF @status = 'pass' SET @passed += 1;
        ELSE IF @status = 'warn'
        BEGIN
            SET @warned += 1;
            PRINT CONCAT('WARN  ', @check_name, ': ', @violations, ' violations');
        END
        ELSE
        BEGIN
            SET @failed += 1;
            PRINT CONCAT('FAIL  ', @check_name, ': ', @violations, ' violations');
        END

        FETCH NEXT FROM check_cursor INTO @check_id, @check_name, @severity, @check_sql;
    END
    CLOSE check_cursor;
    DEALLOCATE check_cursor;

    PRINT CONCAT('=== Quality checks: ', @passed, ' passed, ', @warned, ' warned, ',
                 @failed, ' failed ===');

    IF @failed > 0
    BEGIN
        DECLARE @msg NVARCHAR(2000) =
            CONCAT('Data quality gate failed: ', @failed, ' error-severity check(s) violated.');
        THROW 50001, @msg, 1;
    END
END;
GO

PRINT 'etl.run_quality_checks created.';
GO
