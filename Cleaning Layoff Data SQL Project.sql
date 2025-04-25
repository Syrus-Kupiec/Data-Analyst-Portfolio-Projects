-- Data Cleaning

SELECT *
FROM world_layoffs.layoffs;

-- 1. Remove duplicates if any
-- 2. Standardize data
-- 3. Null values or blank values
-- 4. Remove any unnecessary columns and rows


-- ///// Create staging table
CREATE TABLE layoffs_staging3
LIKE layoffs3;

SELECT *
FROM layoffs_staging3;

INSERT layoffs_staging3
SELECT *
FROM layoffs3;

-- ///// Finding and deleting duplicate rows
SELECT *,
ROW_NUMBER()
OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging3;


WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging3
)
SELECT *
FROM duplicate_cte
WHERE row_num >= 2;
-- Found duplicates, now delete them

-- Create new staging table to add row number
CREATE TABLE `layoffs_staging4` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Populate new table with row numbers and delete duplicate data
SELECT *
FROM layoffs_staging4;

INSERT INTO layoffs_staging4
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging3;

DELETE
FROM layoffs_staging4
WHERE row_num > 1;

-- ///// Standardizing data

-- TRIM removes spaces before and after
SELECT company, TRIM(company)
FROM layoffs_staging4;

UPDATE layoffs_staging4
SET company = TRIM(company);

-- Standardizing Industry values
SELECT DISTINCT industry
FROM layoffs_staging4
ORDER BY 1;

SELECT *
FROM layoffs_staging4
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging4
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardizing Country values
SELECT DISTINCT country
FROM layoffs_staging4
ORDER BY 1;

SELECT *
FROM layoffs_staging4
WHERE country LIKE 'United States%';

UPDATE layoffs_staging4
SET country = 'United States'
-- Could also do
-- SET country = TRIM(TRAILING '.' FROM country)
-- To remove the . at the end
WHERE country LIKE 'United States%';

-- Standardizing Date values
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging4;
-- If some are null, could be due to trailing spaces or others, try TRIM

UPDATE layoffs_staging4
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging4
MODIFY COLUMN `date` DATE;

-- ///// NULL and empty values
SELECT *
FROM layoffs_staging4
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Finding null in industry
SELECT *
FROM layoffs_staging4
WHERE industry IS NULL
OR industry = '';

-- Found industry and populated the null value
SELECT *
FROM layoffs_staging4
WHERE company = 'Airbnb';

UPDATE layoffs_staging4
SET industry = 'Travel'
WHERE company = 'Airbnb';

-- Did not find and industry
SELECT *
FROM layoffs_staging4
WHERE company = "Bally's Interactive";

-- Found industry and populated the null value
SELECT *
FROM layoffs_staging4
WHERE company = "Carvana";

UPDATE layoffs_staging4
SET industry = 'Transportation'
WHERE company = 'Carvana';

-- Found industry and populated the null value
SELECT *
FROM layoffs_staging4
WHERE company = "Juul";

UPDATE layoffs_staging4
SET industry = 'Consumer'
WHERE company = 'Juul';

-- More efficient way, self join update blank and null with populated values with same company and location
SELECT *
FROM layoffs_staging4 AS t1
JOIN layoffs_staging4 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL OR t2.industry != '');

UPDATE layoffs_staging4 AS t1
JOIN layoffs_staging4 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL OR t2.industry != '');

-- Deleting un-usable data rows
SELECT *
FROM layoffs_staging4
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- No data on laid off and cannot populate so data is useless
DELETE
FROM layoffs_staging4
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Row number row is not needed anymore after duplicates have been deleted
SELECT *
FROM layoffs_staging4;

ALTER TABLE layoffs_staging4
DROP COLUMN row_num;

-- DATA IS CLEAN!


