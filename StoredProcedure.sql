create or replace procedure asof_join (
                                       TABLE_A          string
                                      ,TABLE_B          string
                                      ,ENTITY_A         string
                                      ,ORDER_BY_A       string
                                      ,ENTITY_B         string
                                      ,ORDER_BY_B       string
                                      ,A_COLUMNS        array
                                      ,B_COLUMNS        array
                                      )
returns string
language javascript
as
$$

"option strict"

var cols = getColumns(A_COLUMNS, B_COLUMNS, ENTITY_A, ORDER_BY_A, ENTITY_B, ORDER_BY_B);
var union = getUnion(TABLE_A, TABLE_B, ENTITY_A, ORDER_BY_A, ENTITY_B, ORDER_BY_B, A_COLUMNS, B_COLUMNS);

return `${union}select${cols}from UNIONED qualify SOURCE_TABLE = 'A';`;

// ---------

function getColumns(aCols, bCols, partitionBy, orderBy) {

    var i = 0;
    var sql = "";

    
    sql  = ` "AB_${partitionBy}"\n`;
    sql += `      ,"AB_${orderBy}"\n`;
    sql += `      ,lag(iff(SOURCE_TABLE = 'A', null, "AB_${orderBy}")) ignore nulls over (partition by "AB_${partitionBy}" order by "AB_${orderBy}") "B_${orderBy}"\n`;
    
    for (i = 0; i < aCols.length; i++) {
        sql += `      ,"A_${aCols[i]}" as "A_${aCols[i]}"\n`;
    }

    for (i = 0; i < bCols.length; i++) {
        sql += `      ,lag("B_${bCols[i]}") ignore nulls over (partition by "AB_${partitionBy}" order by "AB_${orderBy}") as "B_${bCols[i]}"\n`;
    }

    return sql;

}

function getUnion(tableA, tableB, partitionByA, orderByA, partitionByB, orderByB, aCols, bCols) {

    var i = 0;
    var aList;
    var bList;

    aList = `      ,"${partitionByA}" as "AB_${partitionByA}"\n      ,"${orderByA}" as "AB_${orderByA}"`;
    bList = `      ,"${partitionByB}" as "AB_${partitionByA}"\n      ,"${orderByB}" as "AB_${orderByA}"`;
    
    for (i = 0; i < aCols.length; i++) {
        aList += `\n      ,"${aCols[i]}" as "A_${aCols[i]}"`;
        bList += `\n      ,NULL as "A_${aCols[i]}"`;
    }

    for (i = 0; i < bCols.length; i++) {
        bList += `\n      ,"${bCols[i]}" as "B_${bCols[i]}"`;
        aList += `\n      ,NULL as "B_${bCols[i]}"`;
    }

    var sql = `with UNIONED as
(
select 'A' as SOURCE_TABLE 
${aList}
from   "${tableA}"
union all
select 'B' as SOURCE_TABLE
${bList}
from   "${tableB}"
)
`;

return sql;
}

$$;
