-- create or replace view V_FACT_SALE(
--                                    fact_id,
--                                    product_id,
--                                    product_name,
--                                    sales_order_id,
--                                    manager_id,
--                                    manager_first_name,
--                                    manager_last_name,
--                                    office_id,
--                                    office_name,
--                                    city_id,
--                                    city_name,
--                                    country,
--                                    region,
--                                    sale_qty,
--                                    sale_price,
--                                    sale_amount,
--                                    sale_date
--     ) as
-- select ol.ORDER_LINE_ID,
--        ol.product_id,
--        p.PRODUCT_NAME,
--        so.SALES_ORDER_ID,
--        so.manager_id,
--        m.MANAGER_FIRST_NAME,
--        m.MANAGER_LAST_NAME,
--        m.office_id,
--        o.OFFICE_NAME,
--        o.city_id,
--        c.CITY_NAME,
--        c.COUNTRY,
--        c.REGION,
--        ol.product_qty,
--        ol.product_price,
--        ol.product_qty * ol.product_price,
--        so.order_date
-- from sales_order_line ol
--          left outer join sales_order so on (so.sales_order_id = ol.sales_order_id)
--          left outer join manager m on (m.manager_id = so.manager_id)
--          left outer join office o on (m.office_id = o.office_id)
--          left outer join city c on (o.CITY_ID = c.city_id)
--          left outer join product p on (p.PRODUCT_ID = ol.PRODUCT_ID);

-- 1) Каждый месяц компания выдает премию в размере 5% от суммы продаж менеджеру, который за предыдущие 3 месяца
-- продал товаров на самую большую сумму. Выведите месяц, manager_id, manager_first_name, manager_last_name,
-- премию за период с января по декабрь 2014 года

-- реализация с разбора
    --1.подготовка сырых данных
        --все менеджеры и месяцы
--Task1
with ALL_MANAGERS as (
        select distinct MANAGER_ID, MANAGER_FIRST_NAME, MANAGER_LAST_NAME
        from V_FACT_SALE
     ),
     ALL_MONTHS as (
        select add_months(to_date('01.10.2013', 'DD.MM.YYYY'), level - 1) SALE_MONTH
        from dual connect by level <= 15
     ),
     ALL_MANAGERS_MONTHS as (
         select SALE_MONTH, MANAGER_ID, MANAGER_FIRST_NAME, MANAGER_LAST_NAME
         from ALL_MANAGERS cross join ALL_MONTHS
     ),
     --выбор нужных данных
     ST_1 as (
        select SALE_DATE, MANAGER_FIRST_NAME, MANAGER_LAST_NAME, MANAGER_ID, SALE_AMOUNT
        from V_FACT_SALE
        where SALE_DATE between to_date('2013.10.01', 'YYYY.MM.DD') and to_date('2014.12.31', 'YYYY.MM.DD')
     ),
        --группировка данных и сумма по месяцам
            --т.к. справа (ALL_MANAGERS_MONTHS M) всегда есть значения полей - берем справа
     ST_2 as (
         select M.MANAGER_ID, M.MANAGER_FIRST_NAME, M.MANAGER_LAST_NAME, M.SALE_MONTH,
                sum(S.SALE_AMOUNT) S_SALE_AMOUNT
         from ST_1 S right outer join ALL_MANAGERS_MONTHS M
             on(S.MANAGER_ID = M.MANAGER_ID and trunc(S.SALE_DATE, 'MM') = M.SALE_MONTH)
         group by M.MANAGER_ID, M.MANAGER_FIRST_NAME, M.MANAGER_LAST_NAME, M.SALE_MONTH
     ),
     --2.Сумма за три пред-х месяца
     ST_3 as (
         select MANAGER_ID, MANAGER_FIRST_NAME, MANAGER_LAST_NAME, SALE_MONTH,
                sum(S_SALE_AMOUNT) over (partition by MANAGER_ID
                    order by SALE_MONTH
                    range between interval '3' month preceding and interval '1' month preceding ) PREV_SALE_AMOUNT
         from ST_2 --where MANAGER_ID=1 --проверка
     ),
     ST_4 as (
         select MANAGER_ID, MANAGER_FIRST_NAME, MANAGER_LAST_NAME, SALE_MONTH, PREV_SALE_AMOUNT,
                max(PREV_SALE_AMOUNT) over (partition by SALE_MONTH) MAX_SALE_AMOUNT
         from ST_3
     )
select MANAGER_ID, MANAGER_FIRST_NAME, MANAGER_LAST_NAME, SALE_MONTH, MAX_SALE_AMOUNT * 0.05
from ST_4
where PREV_SALE_AMOUNT = MAX_SALE_AMOUNT
  and SALE_MONTH >= to_date('2014.01.01', 'YYYY.MM.DD');

-- моя реализация до разбора

-- недостатки: 1.если в текущем месяце менеджер не продавал, то он не учитывается в выборке и
-- дальнейших манипуляциях, даже если по сумме предыд-х месяцев все равно лучший.
-- 2. Усложнение программы аналитическими функциями там, где можно было использовать агрегирующие;

-- в целом задаче была решена в другой логике начисления надбавки
-- (которая не противоречит условию задачи): предполагалось вычисление от суммы продаж за текущий месяц,
-- а не за три предыдущих из-за чего выделенный недостаток даже не рассматривался возможным

--PreTask1
select *
from (
         select MANAGER_ID,
                MANAGER_FIRST_NAME,
                MANAGER_LAST_NAME,
                MONTH,
                SUM_PER_MONTH * 0.05             BONUS,
                SUM_PREV_THREE_MNTH,
                rank() over (partition by MONTH
                    order by SUM_PREV_THREE_MNTH desc) BONUS_RANK
        --считаем сумму за предыдущие три месяца для месяца в текущей строке
         from (
                  select MANAGER_ID,
                         MANAGER_FIRST_NAME,
                         MANAGER_LAST_NAME,
                         SUM_PER_MONTH,
                         MONTH,
                         sum(SUM_PER_MONTH)
                             over (partition by MANAGER_ID
                                 order by MONTH
                                 range between interval '3' month preceding and interval '1' month preceding)
                             as SUM_PREV_THREE_MNTH
                  from (
                           select MANAGER_ID,
                                  MANAGER_FIRST_NAME,
                                  MANAGER_LAST_NAME,
                                  MONTH,
                                  -- в случае нескольких продаж за месяц из-за структуры таблицы вывода
                                  -- аналитического запроса
                                  MAX(SUM_PER_MONTH) SUM_PER_MONTH
                           from (
                                    select MANAGER_ID,
                                           MANAGER_FIRST_NAME,
                                           MANAGER_LAST_NAME,
                                           SALE_DATE,
                                           trunc(SALE_DATE, 'MM')                                              as MONTH,
                                           sum(SALE_AMOUNT)
                                               over (partition by MANAGER_ID
                                                   order by SALE_DATE
                                                   range between interval '1' month preceding and current row) as SUM_PER_MONTH
                                    from V_FACT_SALE
                                    where SALE_DATE > to_date('2013.09.30', 'YYYY.MM.DD')
                                      and SALE_DATE < to_date('2015.01.01', 'YYYY.MM.DD')

                                )
                           group by MANAGER_ID, MANAGER_FIRST_NAME, MANAGER_LAST_NAME, MONTH
                       )
              )
         where SUM_PREV_THREE_MNTH is not null
     )
where BONUS_RANK = 1
  and MONTH > to_date('2013.12.31', 'YYYY.MM.DD')
  and MONTH < to_date('2015.01.01', 'YYYY.MM.DD')
order by MONTH;


-- 2) Компания хочет оптимизировать количество офисов, проанализировав относительные объемы продаж
-- по офисам в течение периода с 2013-2014 гг. Выведите год, office_id, city_name, country,
-- относительный объем продаж за текущий год. Офисы, которые демонстрируют наименьший относительной объем в течение двух
-- лет скорее всего будут закрыты.

--Task2
with DATA_INIT as (
        select OFFICE_ID, OFFICE_NAME, CITY_NAME, COUNTRY, SALE_AMOUNT, trunc(SALE_DATE, 'YYYY') SALE_YEAR
        from V_FACT_SALE
        where SALE_DATE between to_date('01.01.2013', 'DD.MM.YYYY')
            and to_date('31.12.2014', 'DD.MM.YYYY')
    ),
     -- т.к. разное количество офисов с продажами за 13-й и 14-ый годы
     ALL_YEARS as (
         select add_months(to_date('01.01.2013', 'DD.MM.YYYY'), level - 1) SALE_MONTH
         from dual connect by level <= 24
     ),
     ALL_OFFICES_YEARS as (
         select *
         from ( select distinct trunc(SALE_MONTH, 'YYYY') SALE_YEAR
               from ALL_YEARS
             ) cross join (
                 select distinct OFFICE_ID, OFFICE_NAME, CITY_NAME, COUNTRY
                 from V_FACT_SALE
                 where SALE_DATE between to_date('01.01.2013', 'DD.MM.YYYY') and to_date('31.12.2014', 'DD.MM.YYYY')
                 and OFFICE_ID is not null
                 )
     ),--
     T_YEAR_AMOUNT as (
         select AOY.OFFICE_ID,
                AOY.OFFICE_NAME,
                AOY.CITY_NAME,
                AOY.COUNTRY,
                AOY.SALE_YEAR,
                NVL(sum(DI.SALE_AMOUNT), 0) YEAR_AMOUNT
         from ALL_OFFICES_YEARS AOY
                  left outer join DATA_INIT DI
                                  on (AOY.OFFICE_ID = DI.OFFICE_ID and AOY.SALE_YEAR = DI.SALE_YEAR)
         group by AOY.SALE_YEAR, AOY.OFFICE_ID, AOY.OFFICE_NAME, AOY.CITY_NAME, AOY.COUNTRY
     ),
     T_YEAR_RATIO as (
         select OFFICE_ID,
                OFFICE_NAME,
                CITY_NAME,
                COUNTRY,
                SALE_YEAR,
                YEAR_AMOUNT,
                ratio_to_report(YEAR_AMOUNT) over (partition by SALE_YEAR) YEAR_RATIO_AMOUNT
         from T_YEAR_AMOUNT
     )
select OFFICE_ID, OFFICE_NAME, CITY_NAME, COUNTRY, SALE_YEAR, YEAR_AMOUNT, YEAR_RATIO_AMOUNT,
       avg(YEAR_RATIO_AMOUNT)
           over (partition by OFFICE_ID, OFFICE_NAME, CITY_NAME, COUNTRY order by SALE_YEAR
               range between interval '1' year preceding and unbounded following ) AVG_RATIO_2013_2014
from T_YEAR_RATIO
order by AVG_RATIO_2013_2014;

-- 3) Для планирования закупок, компания оценивает динамику роста продаж по товарам.
-- Динамика оценивается как отношение объема продаж в текущем месяце к предыдущему.
-- Выведите товары, которые демонстрировали наиболее высокие темпы роста продаж в течение первого полугодия 2014 года.

--Task3
with DATA_INIT as (
    select PRODUCT_ID, PRODUCT_NAME, SALE_AMOUNT, trunc(SALE_DATE, 'MM') MONTH
    from V_FACT_SALE
    where SALE_DATE between to_date('01.12.2013', 'DD.MM.YYYY')
        and to_date('30.06.2014', 'DD.MM.YYYY')
),
     T_MONTH_SUM as (
         select PRODUCT_ID, PRODUCT_NAME, MONTH,
                sum(SALE_AMOUNT) MONTH_AMOUNT
         from DATA_INIT
         group by PRODUCT_ID, PRODUCT_NAME, MONTH
     ),
     T_TREND as (
         select PRODUCT_ID,
                PRODUCT_NAME,
                MONTH,
                MONTH_AMOUNT,
                MONTH_AMOUNT / lag(MONTH_AMOUNT)
                                   over (partition by PRODUCT_ID
                                       order by MONTH) TREND
         from T_MONTH_SUM
         where MONTH > to_date('01.12.2013', 'DD.MM.YYYY')
     )
select distinct PRODUCT_ID, PRODUCT_NAME, TREND_MEDIAN
from (
         select PRODUCT_ID,
                PRODUCT_NAME,
                MONTH,
                TREND,
                median(TREND) over (partition by PRODUCT_ID) TREND_MEDIAN
         from T_TREND
         where TREND is not null
     )
order by TREND_MEDIAN desc;

-- 4) Напишите запрос, который выводит отчет о прибыли компании за 2014 год: помесячно и поквартально.
-- Отчет включает сумму прибыли за период и накопительную сумму прибыли с начала года по текущий период.

--Task4
with DATA_INIT as (
        select SALE_AMOUNT,
               trunc(SALE_DATE, 'MM') SALE_MONTH,
               trunc(SALE_DATE, 'Q') SALE_QUARTER
        from V_FACT_SALE
        where SALE_DATE between to_date('01.01.2014', 'DD.MM.YYYY')
            and to_date('31.12.2014', 'DD.MM.YYYY')
    ),
     -- прибыль компании: сумма продаж всех офисов
     T_OFFICE_SUM as (
         select SALE_MONTH,
                SALE_QUARTER,
                sum(SALE_AMOUNT) as MONTH_SUM
         from DATA_INIT
         group by SALE_MONTH, SALE_QUARTER
     )
select SALE_MONTH,
       SALE_QUARTER,
       MONTH_SUM,
       sum(MONTH_SUM) over (order by SALE_MONTH
           range between unbounded preceding and current row) CUMUL_MONTH_SUM,
       sum(MONTH_SUM) over (order by SALE_QUARTER
           range between unbounded preceding and current row) CUMUL_QUART_SUM
from T_OFFICE_SUM order by SALE_MONTH;

-- 5) Найдите вклад в общую прибыль за 2014 год 10% наиболее дорогих товаров и 10% наиболее дешевых товаров.
-- Выведите product_id, product_name, total_sale_amount, percent

--Task5
with DATA_INIT as (
        select distinct PRODUCT_ID,
                        PRODUCT_NAME,
                        median(SALE_PRICE) over (partition by PRODUCT_ID) MED_SALE_PRICE,
                        sum(SALE_AMOUNT) over (partition by PRODUCT_ID ) SALE_AMOUNT_PRODUCT
               -- т.к товары имеют разную стоимость у разных менеджеров
               -- для формирования выборки с 10% дорогих и 10% дешевых
               -- необходима агрегированная цена товара
        from V_FACT_SALE
        where SALE_DATE between to_date('01.01.2014', 'DD.MM.YYYY')
                  and to_date('31.12.2014', 'DD.MM.YYYY')
    ),
     T_PERCNTLE_PRICE as (
         select PRODUCT_ID,
                PRODUCT_NAME,
                SALE_AMOUNT_PRODUCT,
                percent_rank() over (order by MED_SALE_PRICE) PERCNTLE_PRODUCT_PRICE,
                sum(SALE_AMOUNT_PRODUCT) over () TOTAL_SALE_AMOUNT
         from DATA_INIT
     )
select PRODUCT_ID,
       PRODUCT_NAME,
       SALE_AMOUNT_PRODUCT,
       TOTAL_SALE_AMOUNT,
       SALE_AMOUNT_PRODUCT / TOTAL_SALE_AMOUNT PERCENT_FROM_TOTAL_AMOUNT,
       PERCNTLE_PRODUCT_PRICE
from T_PERCNTLE_PRICE where PERCNTLE_PRODUCT_PRICE not between 0.1 and 0.9;

-- 6) Компания хочет премировать трех наиболее продуктивных (по объему продаж, конечно) менеджеров в каждой стране в 2014 году.
-- Выведите country, <список manager_last_name manager_first_name, разделенный запятыми> которым будет выплачена премия

--Task6
with AMOUNT_MANAGER_YEAR as (
    select MANAGER_ID,
           MANAGER_FIRST_NAME,
           MANAGER_LAST_NAME,
           COUNTRY,
           sum(SALE_AMOUNT) YEAR_SALE_AMOUNT
    from V_FACT_SALE
    where SALE_DATE between to_date('01.01.2014', 'DD.MM.YYYY')
        and to_date('31.12.2014', 'DD.MM.YYYY')
      and COUNTRY is not null
    group by MANAGER_ID, MANAGER_FIRST_NAME, MANAGER_LAST_NAME, COUNTRY
),
     T_COUNTRY_RANK as (
         select MANAGER_ID,
                MANAGER_FIRST_NAME,
                MANAGER_LAST_NAME,
                COUNTRY,
                YEAR_SALE_AMOUNT,
                rank() over (partition by COUNTRY
                    order by YEAR_SALE_AMOUNT desc) BONUS_RANK
         from AMOUNT_MANAGER_YEAR
     )
select COUNTRY,
       listagg(MANAGER_LAST_NAME || ' ' || MANAGER_FIRST_NAME, ', ')
           within group ( order by BONUS_RANK) MANAGERS_TO_BONUS
from T_COUNTRY_RANK where BONUS_RANK <= 3
group by COUNTRY;

-- 7) Выведите самый дешевый и самый дорогой товар, проданный за каждый месяц в течение 2014 года.
-- cheapest_product_id, cheapest_product_name, expensive_product_id, expensive_product_name, month, cheapest_price, expensive_price

--Task7
with DATA_INIT as (
        select PRODUCT_ID,
               PRODUCT_NAME,
               SALE_PRICE,
               trunc(SALE_DATE, 'MM') SALE_MONTH
        from V_FACT_SALE
        where SALE_DATE between to_date('01.01.2014', 'DD.MM.YYYY')
                  and to_date('31.12.2014', 'DD.MM.YYYY')
    ),
     T_HIGHST_LOWST as (
         select distinct SALE_MONTH,
                first_value(SALE_PRICE) over (partition by SALE_MONTH
                    order by SALE_PRICE desc
                    range between unbounded preceding and unbounded following ) HIGHEST_PRICE,
                last_value(SALE_PRICE) over (partition by SALE_MONTH
                    order by SALE_PRICE desc
                    range between unbounded preceding and unbounded following ) LOWEST_PRICE
         from DATA_INIT
     ),
     T_HIGHST_PROD as (
         select DI.PRODUCT_ID,
                DI.PRODUCT_NAME,
                H.HIGHEST_PRICE,
                H.SALE_MONTH
         from DATA_INIT DI
                  inner join T_HIGHST_LOWST H on (DI.SALE_PRICE = H.HIGHEST_PRICE
             and DI.SALE_MONTH = H.SALE_MONTH)
     ),
     T_LOWST_PROD as (
         select DI.PRODUCT_ID,
                DI.PRODUCT_NAME,
                L.LOWEST_PRICE,
                L.SALE_MONTH
         from DATA_INIT DI inner join T_HIGHST_LOWST L on (DI.SALE_PRICE = L.LOWEST_PRICE
             and DI.SALE_MONTH = L.SALE_MONTH)
     )
select H.PRODUCT_ID EXPENSV_PROD_ID,
       H.PRODUCT_NAME EXPENSV_PROD_NAME,
       H.HIGHEST_PRICE,
       L.PRODUCT_ID CHEPST_PROD_ID,
       L.PRODUCT_NAME CHEPST_PROD_NAME,
       L.LOWEST_PRICE,
       H.SALE_MONTH MONTH
from T_HIGHST_PROD H inner join T_LOWST_PROD L on H.SALE_MONTH = L.SALE_MONTH
order by MONTH;

-- 8) Менеджер получает оклад в 30 000 + 5% от суммы своих продаж в месяц. Средняя наценка стоимости товара - 10%
-- Посчитайте прибыль предприятия за 2014 год по месяцам (сумма продаж - (сумма продаж/1.1 + зарплата) = С.прод/11 - з.п)
-- month, sales_amount, salary_amount, profit_amount

--наценка=(продажа - цена)/цена=0.1; 0.1цена = продажа - цена

--Task8
with DATA_INIT as (
    select MANAGER_ID,
           trunc(SALE_DATE, 'MM') MONTH,
           SALE_AMOUNT
    from V_FACT_SALE
    where SALE_DATE between to_date('01.01.2014', 'DD.MM.YYYY') and to_date('31.12.2014', 'DD.MM.YYYY')
),
     T_MONTH_AMOUNT as (
    select distinct MONTH,
              sum(SALE_AMOUNT) over (partition by MONTH)                            SALES_AMOUNT,
              30000 + sum(0.05 * SALE_AMOUNT) over (partition by MANAGER_ID, MONTH) SALARY
    from DATA_INIT
    )
select MONTH, SALES_AMOUNT, sum(SALARY) SALARY_AMOUNT, SALES_AMOUNT/11 - sum(SALARY) PROFIT
from T_MONTH_AMOUNT
group by MONTH, SALES_AMOUNT