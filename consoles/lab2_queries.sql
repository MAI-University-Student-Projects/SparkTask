--1.все заказы
select *
from SALES_ORDER;

--2.все заказы после 01.01.2016
select *
from SALES_ORDER
where ORDER_DATE > to_date('2016.01.01', 'YYYY.MM.DD')
order by ORDER_DATE;

--3.все заказы после 01.01.2016 и до 15.07.2016
select *
from SALES_ORDER
where ORDER_DATE > to_date('2016.01.01', 'YYYY.MM.DD')
  and ORDER_DATE < to_date('2016.07.15', 'YYYY.MM.DD')
order by ORDER_DATE desc;

--4.Найти менеджеров с именем 'Henry'
select *
from MANAGER
where MANAGER_FIRST_NAME = 'Henry';

--5.Выбрать все заказы менеджеров с именем Henry
select *
from SALES_ORDER inner join MANAGER M
    on SALES_ORDER.MANAGER_ID = M.MANAGER_ID
where M.MANAGER_FIRST_NAME = 'Henry';

--6.все уникальные страны из CITY
select distinct COUNTRY
from CITY;

--7.все уникальные комбинации страны и региона из CITY
select distinct COUNTRY, REGION
from CITY
    where REGION is not null;

--8.Выбрать все страны из таблицы CITY с количеством городов в них.
with CITY_COUNTRY as (
    select distinct CITY_NAME, COUNTRY
    from CITY
)
select count(*) as CITY_AMOUNT, COUNTRY
from CITY_COUNTRY
group by COUNTRY;

--9.Выбрать количество товаров (QTY), проданное с 1 по 30 января 2016 года.
select sum(SOL.PRODUCT_QTY) as QTY
from SALES_ORDER SO inner join SALES_ORDER_LINE SOL
    on (SO.SALES_ORDER_ID = SOL.SALES_ORDER_ID and
        SO.ORDER_DATE > to_date('2015.12.31', 'YYYY.MM.DD') and
        SO.ORDER_DATE < to_date('2016.01.31', 'YYYY.MM.DD'))
order by SO.ORDER_DATE DESC;

--10.Выбрать все уникальные названия городов, регионов и стран в одной колонке
select CITY_NAME as TITLE from CITY
union
select COUNTRY from CITY
union
select REGION from CITY
where REGION is not null;

--11.Вывести имена и фамилии менеджер(ов), продавшего товаров в январе 2016 года на наибольшую сумму.
--были допущены ошибки: упущено условие временного периода, sum(SOL.PRODUCT_PRICE))
with INCOME_MANAGER as (
    select sum(SOL.PRODUCT_PRICE * SOL.PRODUCT_QTY) as TOTAL_INCOME, M.MANAGER_ID
    from MANAGER M
        inner join SALES_ORDER SO on (M.MANAGER_ID = SO.MANAGER_ID and
                                      SO.ORDER_DATE > to_date('2015.12.31', 'YYYY.MM.DD') and
                                      SO.ORDER_DATE < to_date('2016.02.01', 'YYYY.MM.DD'))
        inner join SALES_ORDER_LINE SOL on (SO.SALES_ORDER_ID = SOL.SALES_ORDER_ID)
    group by M.MANAGER_ID
)
select IM.TOTAL_INCOME as MAX_INCOME, M.MANAGER_LAST_NAME, M.MANAGER_FIRST_NAME
from INCOME_MANAGER IM
    inner join MANAGER M on (IM.MANAGER_ID = M.MANAGER_ID)
where IM.TOTAL_INCOME in (select max(INCOME_MANAGER.TOTAL_INCOME) from INCOME_MANAGER);