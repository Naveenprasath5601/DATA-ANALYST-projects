-- Exploratory Data Analysis (EDA)

select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select company, sum(total_laid_off)
from layoffs_staging2
Group by company
order by 2 desc;

select min('date'), max('date')
from layoffs_staging2;

select industry, sum(total_laid_off)
from layoffs_staging2
Group by industry
order by 2 desc;

select 'date', sum(total_laid_off)
from layoffs_staging2
group by 'date'
order by 1 desc;

select year ('date'), sum(total_laid_off)
from layoffs_staging2
group by year ('date')
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 1 desc;

select company, sum(percentage_laid_off)
from layoffs_staging2
Group by company
order by 2 desc;

select company, avg(percentage_laid_off)
from layoffs_staging2
Group by company
order by 2 desc;

select *
from layoffs_staging2;

select company, year('date'), sum(total_laid_off)
from layoffs_staging 
group by company, year('date')
order by 3 desc;

with company_year (company, years, total_laid_off) as 
(
select company, year('date'), sum (total_laid_off)
from layoffs_staging2
group by company, year('date')
)
select *, 
dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null 
order by ranking asc;

select *
From layoffs_staging;














