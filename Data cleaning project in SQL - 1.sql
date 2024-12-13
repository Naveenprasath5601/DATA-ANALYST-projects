

select * 
from layoffs;

create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;

with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage
,country, funds_raised) as row_num
from layoffs_staging
)
select *
from duplicate_cte 
where row_num > 1;

select *
from layoffs_staging
where company = 'Casper';

with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage
,country, funds_raised) as row_num
from layoffs_staging
)
delete
from duplicate_cte 
where row_num > 1;




CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL,
   `row_num`int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
where row_num > 1;

insert into layoffs_staging2
SELECT *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised
) as row_num
FROM layoffs_staging;


WITH RankedRows AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions
               ORDER BY id -- or any other column you prefer to use for sorting
           ) AS row_num
    FROM layoffs_staging2
)
DELETE FROM layoffs_staging2
WHERE id IN (
    SELECT id
    FROM RankedRows
    WHERE row_num > 1
);

WITH RankedRows AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, date
               ORDER BY id -- or another column for ordering
           ) AS row_num
    FROM layoffs_staging2
)
DELETE FROM layoffs_staging2
WHERE id IN (
    SELECT id
    FROM RankedRows
    WHERE row_num > 1
);
SELECT *
FROM layoffs_staging2;

-- standardizing data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set conmpany = trim(company);

select distinct industry
from layoffs_staging2
;

select *
from layoffs_staging2
where industry like 'Crypto%';

select distinct country
from layoffs_staging2
order by 1;
 
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') 
WHERE STR_TO_DATE(`date`, '%m/%d/%Y') IS NOT NULL;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

select *
from layoffs_staging2;



select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;






