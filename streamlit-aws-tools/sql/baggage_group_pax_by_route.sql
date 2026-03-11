SELECT base.*,
    MAX(base.uci_rank) OVER (PARTITION BY base.PNR_REF) AS PAX_COUNT
FROM (
    SELECT DISTINCT
        qbgitn.AAIRALPC,
        qbgitn.PRMEFLTN,
        qbgitn.LOCLDEPD,
        qbgitn.DEPUPOR,
        qbgitn.ARVLPOR,
        qt.PNR_REF,
        qbg.BAG_GROUP_BR_ID,
        qbg.TRAVLR_UCI,
        RIGHT('0000' || TRIM(qb.BAG_TAG_CARRIER_NUMBER), 4) || '-' || TRIM(qb.BAG_TAG_NUMBER) AS TAG_NUMBER,
        qb.BAG_WGT AS WEIGHT_KG,
        qb.BAG_CABIN_CODE,
        qbg.BAG_GROUP_CHECKIN_WGT,
        qbg.BAG_GROUP_CHECKIN_PIECE,
        qbg.BAG_GROUP_POOL_IND,
        DENSE_RANK() OVER (PARTITION BY qt.PNR_REF ORDER BY qbg.TRAVLR_UCI) AS uci_rank
    FROM PQMF.QHBS300_BAG_GROUP qbg
    JOIN PQMF.QHB0303_BAG_GROUP_MEMBER qbgm
      ON qbg.BAG_GROUP_BR_ID = qbgm.BAG_GROUP_BR_ID
    JOIN PQMF.QHB0302_BAG_GROUP_ITIN qbgitn
      ON qbgm.BAG_GROUP_BR_ID = qbgitn.BAG_GROUP_BR_ID
    JOIN PQMF.QHBS304_BAG qb
      ON qbgm.BAG_UBI_ID = qb.BAG_UBI_ID
    JOIN PQMF.QHBS200_TRAVELLER qt
      ON qbg.TRAVLR_UCI = qt.TRAVLR_UCI
    WHERE
        (qbgitn.AAIRALPC = NULLIF(TRIM(?), '') OR NULLIF(TRIM(?), '') IS NULL)
    AND (qbgitn.PRMEFLTN = NULLIF(TRIM(?), '') OR NULLIF(TRIM(?), '') IS NULL)
    AND (qbgitn.DEPUPOR  = NULLIF(TRIM(?), '') OR NULLIF(TRIM(?), '') IS NULL)
    AND (qbgitn.ARVLPOR  = NULLIF(TRIM(?), '') OR NULLIF(TRIM(?), '') IS NULL)
    AND (qbgitn.LOCLDEPD >= ? OR ? IS NULL)
    AND (qbgitn.LOCLDEPD <= ? OR ? IS NULL)
    AND (qb.BAG_CABIN_CODE = NULLIF(TRIM(?), '') OR NULLIF(TRIM(?), '') IS NULL)
) base
ORDER BY base.LOCLDEPD DESC, base.PNR_REF, base.TRAVLR_UCI;