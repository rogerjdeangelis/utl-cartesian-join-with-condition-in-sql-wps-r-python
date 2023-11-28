# utl-cartesian-join-with-condition-in-sql-wps-r-python
Cartesian join with condition in wps r python
    %let pgm=utl-cartesian-join-with-condition-in-sql-wps-r-python;

    Cartesian join with condition in wps r python

         1. r native
         2. r sql
         3, wps sql
         4. python sql

    github
    https://tinyurl.com/mrxmz3ew
    https://github.com/rogerjdeangelis/utl-cartesian-join-with-condition-in-sql-wps-r-python

    stackoverflow
    https://stackoverflow.com/questions/77552333/join-by-data-masking-in-dplyr-in-r

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */
    libname sd1 "d:/sd1";
    data sd1.hav1st(keep=a) sd1.hav2nd(keep=b);
      do a=1 to 4;
         b=a;
         output;
      end;
    run;quit;

    /**************************************************************************************************************************/
    /*               |                                                                                                        */
    /*  SD1.HAV1ST   |   SD1.HAV2ND                                                                                           */
    /*               |                                                                                                        */
    /*  Obs    A     |    Obs    B                                                                                            */
    /*               |                                                                                                        */
    /*   1     1     |     1     1                                                                                            */
    /*   2     2     |     2     2                                                                                            */
    /*   3     3     |     3     3                                                                                            */
    /*   4     4     |     4     4                                                                                            */
    /*               |                                                                                                        */
    /**************************************************************************************************************************/

    /*                      _   _
    / |  _ __   _ __   __ _| |_(_)_   _____
    | | | `__| | `_ \ / _` | __| \ \ / / _ \
    | | | |    | | | | (_| | |_| |\ V /  __/
    |_| |_|    |_| |_|\__,_|\__|_| \_/ \___|

    */

    /*----                                                                   ----*/
    /*---- Not sure I know what is going on in R                             ----*/
    /*----                                                                   ----*/

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64('
    libname sd1 "d:/sd1";
    proc  r;
    export data=sd1.hav1st r=hav1st;
    export data=sd1.hav2nd r=hav2nd;
    submit;
    library(dplyr);
    a <- hav1st;
    b <- hav2nd;
    i <- 1;
    a_var <- paste0("a.", i);
    b_var <- paste0("b.", i);
    want<-inner_join(a, b, join_by(x$A < y$B));
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                              |                                                                                         */
    /*            INPUTS            |                  OUTPUTS                                                                */
    /*                              |                                                                                         */
    /*   SD1.HAV1ST     SD1.HAV2nd  |    The WPS R System |   WPS/SAS                                                         */
    /*                              |                     |                                                                   */
    /*   Obs    A       Obs    B    |      A B            |   Obs    A    B                                                   */
    /*                              |                     |                                                                   */
    /*    1     1        1     1    |    1 1 2            |    1     1    2                                                   */
    /*    2     2        2     2    |    2 1 3            |    2     1    3                                                   */
    /*    3     3        3     3    |    3 1 4            |    3     1    4                                                   */
    /*    4     4        4     4    |    4 2 3            |    4     2    3                                                   */
    /*                              |    5 2 4            |    5     2    4                                                   */
    /*                              |    6 3 4            |    6     3    4                                                   */
    /*                              |                     |                                                                   */
    /**************************************************************************************************************************/

    /*___                     _
    |___ \   _ __   ___  __ _| |
      __) | | `__| / __|/ _` | |
     / __/  | |    \__ \ (_| | |
    |_____| |_|    |___/\__, |_|
                           |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64('
    libname sd1 "d:/sd1";
    proc  r;
    export data=sd1.hav1st r=hav1st;
    export data=sd1.hav2nd r=hav2nd;
    submit;
    library(sqldf);
    want<-sqldf("
      select
          a
         ,b
      from
         hav1st inner join hav2nd
      on
         a < b
    ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    ');

    run;quit;

    proc print data=sd1.want;
    run;quit;

    /*____                                  _
    |___ /  __      ___ __  ___   ___  __ _| |
      |_ \  \ \ /\ / / `_ \/ __| / __|/ _` | |
     ___) |  \ V  V /| |_) \__ \ \__ \ (_| | |
    |____/    \_/\_/ | .__/|___/ |___/\__, |_|
                     |_|                 |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc  sql;
      create
          table sd1.want as
      select
          a
         ,b
      from
         sd1.hav1st inner join sd1.hav2nd
      on
         a < b
    ;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /*  _                 _   _                             _
    | || |    _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
    | || |_  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
    |__   _| | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
       |_|   | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
             |_|    |___/                                |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    proc python;
    export data=sd1.hav1st python=hav1st;
    export data=sd1.hav2nd python=hav2nd;
    submit;
    import pyreadstat;
    from os import path;
    import pandas as pd;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    want = pdsql('''
      select
          a
         ,b
      from
         hav1st as l inner join hav2nd as r
      on
         a < b
    ''');
    print(want);
    pyreadstat.write_xport(want, 'd:/xpt/want.xpt',table_name='want',file_format_version=5);
    endsubmit;
    run;quit;
    ");

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;
    run;quit;
    data want;
      set xpt.want;
    run;quit;
    proc print;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* Obs    A    B                                                                                                          */
    /*                                                                                                                        */
    /*  1     1    2                                                                                                          */
    /*  2     1    3                                                                                                          */
    /*  3     1    4                                                                                                          */
    /*  4     2    3                                                                                                          */
    /*  5     2    4                                                                                                          */
    /*  6     3    4                                                                                                          */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
