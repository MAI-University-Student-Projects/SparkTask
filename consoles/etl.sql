-- vopros=posttypeid(1), answer=posttypeid(2)

-- select min(creationdate), select max(creationdate), max(deletiondate), max(closeddate) from SO_RU.posts;
-- 2010-10-10, null, 2018-09-02

create sequence SEQ_DIM_DATE;

declare
    cdate date := to_date('01.10.2010', 'DD.MM.YYYY');
begin
    loop
        insert into DIM_DATE(DIM_DATE_ID, DATE_VAL, DATE_LVL)
        values (SEQ_DIM_DATE.nextval, cdate, 1);
        if (to_char(cdate, 'DD') = '01') then
            insert into DIM_DATE(DIM_DATE_ID, DATE_VAL, DATE_LVL)
            values (SEQ_DIM_DATE.nextval, trunc(cdate, 'MM'), 2);
        end if;
        if (to_char(cdate, 'DD.MM') = '01.01') then
            insert into DIM_DATE(DIM_DATE_ID, DATE_VAL, DATE_LVL)
            values (SEQ_DIM_DATE.nextval, trunc(cdate, 'YYYY'), 3);
        end if;
        cdate := cdate + 1;
        exit when cdate >= to_date('01.12.2019', 'DD.MM.YYYY');
    end loop;
end;

-- declare
--     cdate date := to_date('01.10.2010', 'DD.MM.YYYY');
-- begin
--     loop
--         insert into DIM_DATE(DIM_DATE_ID, DATE_VAL, DATE_LVL)
--         values (SEQ_DIM_DATE.nextval, to_char(cdate, 'DD.MM.YYYY'), 1);
--         if (to_char(cdate, 'DD') = '01') then
--             insert into DIM_DATE(DIM_DATE_ID, DATE_VAL, DATE_LVL)
--             values (SEQ_DIM_DATE.nextval, to_char(cdate, 'MM.YYYY'), 2);
--         end if;
--         if (to_char(cdate, 'DD.MM') = '01.01') then
--             insert into DIM_DATE(DIM_DATE_ID, DATE_VAL, DATE_LVL)
--             values (SEQ_DIM_DATE.nextval, to_char(cdate, 'YYYY'), 3);
--         end if;
--         cdate := cdate + 1;
--         exit when cdate >= to_date('01.12.2019', 'DD.MM.YYYY');
--     end loop;
-- end;
-- у меня не было разрешения на materilized view
create or replace view MV_QUESTION_TOTAL ( Q_ID,
    Q_CREATION_DATE,
    Q_VIEWCOUNT,
    Q_SCORE,
    Q_ANSWERCOUNT,
    Q_TAG_ID,
    Q_TAG_NAME )
as
(
    select P.ID,
           P.CREATIONDATE,
           P.VIEWCOUNT,
           P.SCORE,
           P.ANSWERCOUNT,
           coalesce(SN_ORG.ORIG_ID, T.ID) as TAG_ID,
           coalesce(SN_ORG.ORIG_NAME, T.TAGNAME) as TAG_NAME
    from SO_RU.posts P
        left outer join SO_RU.POSTTAGS P_TAG on P.ID = P_TAG.POSTID
        left outer join SO_RU.TAGS T on P_TAG.TAGID = T.ID
        left outer join (
            select TSYN.ID     as SYN_ID,
                   TSYN.TAGNAME as SYN_NAME,
                   TORIG.ID as ORIG_ID,
                   TORIG.TAGNAME as ORIG_NAME
            from SO_RU.TAGSYNONYMS TAGSYN
                inner join SO_RU.TAGS TSYN on TAGSYN.SOURCETAGNAME = TSYN.TAGNAME
                inner join SO_RU.TAGS TORIG on TAGSYN.TARGETTAGNAME = TORIG.TAGNAME
        ) SN_ORG on T.ID = SN_ORG.SYN_ID
    where P.posttypeid = 1
);
-- where pst.posttypeid = 1 and tagid is null/and TAGNAME is null
-- есть вопросы с неправильными тегами, 3 штуки

insert into DIM_TAGS(DIM_TAG_ID, TAG_NAME)
select distinct NVL(Q_TAG_ID, -1), NVL(Q_TAG_NAME, 'no_tag')
from MV_QUESTION_TOTAL;

commit;

-- select *
-- from DIM_TAGS
--     right join SO_RU.TAGSYNONYMS on (DIM_TAGS.TAG_NAME = TARGETTAGNAME)
-- where DIM_TAG_ID is null
--9 несовпадений(все эти 9 TARGETTAGNAME аналогично отсутствуют в SO_RU.tags)

create sequence SEQ_FACT_QST;

insert into FACT_QUESTIONED (
                             D_DATE_ID,
                             D_TAG_ID,
                             FACT_ID,
                             VIEWS_AMOUNT, ANSWERS_AMOUNT, TOTAL_AMOUNT,
                             SCORE)
select DD.DIM_DATE_ID,
       DT.DIM_TAG_ID,
       SEQ_FACT_QST.nextval,
       QT.Q_VIEWCOUNT,
       QT.Q_ANSWERCOUNT,
       1,
       QT.Q_SCORE
from MV_QUESTION_TOTAL QT
    left outer join DIM_DATE DD
        on (trunc(QT.Q_CREATION_DATE, 'DD') = DD.DATE_VAL and DD.DATE_LVL = 1)
    left outer join DIM_TAGS DT on (NVL(QT.Q_TAG_NAME, 'no_tag') = DT.TAG_NAME);
-- чтобы примеры работали на других уровнях дат, необходимо было расписать случаи с
-- <trunc(QT.Q_CREATION_DATE, 'MM'), DATE_LVL = 2>
-- <trunc(QT.Q_CREATION_DATE, 'YYYY'), DATE_LVL = 3>

commit;
create table STG_FACT_QUESTIONED as select * from FACT_QUESTIONED;

insert into FACT_QUESTIONED
select D_DATE_ID, D_TAG_ID, -5, AVG(VIEWS_AMOUNT),
       SUM(ANSWERS_AMOUNT), SUM(TOTAL_AMOUNT), AVG(SCORE)
from STG_FACT_QUESTIONED
group by cube (D_DATE_ID, D_TAG_ID);

commit;

--пример (не работает на других DATE_LVL)
select DD.DATE_VAL as DATE_CREATED,
       DT.TAG_NAME as TAG,
       VIEWS_AMOUNT as AVG_VIEWS, TOTAL_AMOUNT, SCORE as AVG_SCORE
from FACT_QUESTIONED FQ
    inner join DIM_DATE DD on DD.DIM_DATE_ID = FQ.D_DATE_ID
    inner join DIM_TAGS DT on DT.DIM_TAG_ID = FQ.D_TAG_ID
where FQ.FACT_ID = -5 and DATE_VAL = to_date('21.07.2017', 'DD.MM.YYYY')

select * from MV_QUESTION_TOTAL