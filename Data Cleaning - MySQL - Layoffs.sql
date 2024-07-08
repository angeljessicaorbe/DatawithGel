-- SQL Project - Data Cleaning
-- Data Source: https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT *
FROM layoffs;

-- First thing we want to do is create a staging table. 
-- This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- Now when we are data cleaning we usually follow a few steps
-- 1. Check if there's a duplicates and remove any
-- 2. Standardize data and fix errors
-- 3. Look at null values and see what to do with them
-- 4. Remove any columns and rows that are not necessary - few ways

-- 1. Remove Duplicates

# First let's check for duplicates, we are going to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column.
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location_state` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date_of_layoff` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location_state, industry, total_laid_off,percentage_laid_off, date_of_layoff, stage, country, funds_raised) AS row_num
FROM layoffs_staging;


# With these we discovered that there are two companies with duplicate entries.
SELECT *
FROM layoffs_staging2
WHERE row_num >1;

# We have deleted the duplicates using DELETE function
DELETE
FROM layoffs_staging2
WHERE row_num >1;


-- 2. Standardize data and fix errors

# We start by standardizing the column 'company'. 
# Using TRIM to take the white space off before and after the entries
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT company
FROM layoffs_staging2
ORDER BY 1;

# Now, we take a look at the column 'industry', where we find out that "Ebay's" industry is a link. 
# We now categorize Ebay under Retail.
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Retail'
WHERE industry LIKE 'https://www.calcalistech.com/ctechnews/article/rysmrkfua';

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

# Now, looking at location state, i discovered weird characters and update them accordinly.
SELECT DISTINCT(location_state)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE location_state = 'F?rde';

# Updated the weird locations such as 'D?sseldorf', 'F?rde'
SELECT DISTINCT location_state
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET location_state = 'Døsseldorf'
WHERE location_state LIKE 'D?sseldorf';

UPDATE layoffs_staging2
SET location_state = 'Førde'
WHERE location_state LIKE 'F?rde';

UPDATE layoffs_staging2
SET location_state = 'Wroclaw'
WHERE location_state LIKE 'Wroc?aw';


# Looking at the country, and it looks like it's clean already
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


# Changing date column to date data type AND altering the data type of date column from text to date
SELECT date_of_layoff
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date_of_layoff = str_to_date(date_of_layoff, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN date_of_layoff DATE;

-- 3. Dealing with nulls

SELECT *
FROM layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = '';

SELECT *
FROM layoffs_staging2
WHERE industry = ''
OR industry IS NULL;

-- Discovered that Appsmith industry is missing, so we need to update it to Other since it does not fall on the given categories

UPDATE layoffs_staging2
SET industry = 'Other'
WHERE industry LIKE '';

-- 4. Delete entries that has missing values for laid off and percentage laid off
DELETE
FROM layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = '';


-- Deleting row num since we don't need row number anymore
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
