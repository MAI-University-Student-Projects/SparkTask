with FULL_TREE as (
    select ID,
           FILE_SIZE,
           TYPE,
           PARENT_ID,
           LPAD(' ', (level - 1) * 4) || SYS_CONNECT_BY_PATH(NAME, '\') as FULL_NAME
    from FILE_SYSTEM
    connect by prior ID = PARENT_ID
    start with PARENT_ID IS NULL
),
     ALL_AS_ROOTS_SIZES as (
         select ROOT_ID, sum(F_SIZE) as DIR_SIZE
         from (
                  select ID,
                         NVL(FILE_SIZE, 0) F_SIZE,
                         connect_by_root ID ROOT_ID
                  from FILE_SYSTEM
                  connect by prior ID = PARENT_ID
              )
         group by ROOT_ID
     )
select FT.ID, AARS.DIR_SIZE, FT.TYPE, FT.PARENT_ID,
       ratio_to_report(AARS.DIR_SIZE) over (partition by FT.PARENT_ID) as DIR_RATIO,
       FT.FULL_NAME
from ALL_AS_ROOTS_SIZES AARS inner join FULL_TREE FT
    on AARS.ROOT_ID = FT.ID
order by FT.ID