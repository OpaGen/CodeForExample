CREATE MULTISET TABLE bas.ann_cntr_for_otchet_opazd_cntr_history ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      RC_CNTR_ID INTEGER,
      CNTR_ID INTEGER,
      AVG_DELAY_DAYS FLOAT,
      DAY_ID DATE FORMAT 'YYYY-MM-DD')
PRIMARY INDEX ( RC_CNTR_ID ,CNTR_ID ,DAY_ID );