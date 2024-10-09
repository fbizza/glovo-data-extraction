with subway_orders as (
    SELECT order_id
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE store_id IN (60087, 60264, 142427, 142594, 142645, 142647, 142653, 142659, 142667, 142690, 142707, 143021, 143378, 156135, 156166, 156190, 182324, 212091, 212100, 212180, 212368, 242774, 242826, 242862, 243015, 243038, 243055, 278061, 278539, 302751, 302795, 309910, 313264, 316224, 317149, 325298, 325364, 325371, 336065, 344446, 365743, 378600, 378602, 427344, 434110)
      AND (date_trunc('day', order_started_local_at)) >= (DATE '2024-07-01')
      AND (date_trunc('day', order_started_local_at)) <= (DATE '2024-08-30')
)
SELECT date (date_trunc('week', order_started_local_at)) AS week,
       count(order_id) as orders,
       AVG(contribution_margin_eur) AS avg_cm,
       AVG(TCOREV_eur) as TCOREV_eur,
       AVG(TOTBDR_eur) as TOTBDR_eur,
       AVG(MBSRSU_eur) as MBSRSU_eur,
       AVG(BWSREV_eur) as BWSREV_eur,
       AVG(DEACSU_eur) as DEACSU_eur,
       AVG(ADVE_eur) as ADVE_eur,
       AVG(SERFIG_eur) as SERFIG_eur,
       AVG(MFCGSO_eur) as MFCGSO_eur,
       AVG(QMCOGS_eur) as QMCOGS_eur,
       AVG(CPOBIF_eur) as CPOBIF_eur,
       AVG(INHD_eur) as INHD_eur,
       AVG(FGRC_eur) as FGRC_eur,
       AVG(MESH_eur) as MESH_eur,
       AVG(OTMR_eur) as OTMR_eur,
       AVG(SOFT_eur) as SOFT_eur,
       AVG(WASTAG_eur) as WASTAG_eur,

       AVG(CRCARE_eur) as CRCARE_eur,
       AVG(RECOOK_eur) as RECOOK_eur,
       AVG(RECOOC_eur) as RECOOC_eur,
       AVG(ASBYPA_eur) as ASBYPA_eur,
       AVG(OVHE_eur) as OVHE_eur,
       AVG(MFCL_eur) as MFCL_eur,
       AVG(MFMC_eur) as MFMC_eur,
       AVG(PAYFEE_eur) as PAYFEE_eur,
       AVG(CUSPEC_eur) as CUSPEC_eur,
       AVG(CUSNOC_eur) as CUSNOC_eur,
       AVG(FSUBDF_eur) as FSUBDF_eur,
       AVG(GROSUF_eur) as GROSUF_eur,
       AVG(NGRSUF_eur) as NGRSUF_eur,
       AVG(CONSUF_eur) as CONSUF_eur,

       AVG(MFCSUF_eur) as MFCSUF_eur,
       AVG(FOPICK_eur) as FOPICK_eur,
       AVG(F2DDRM_eur) as F2DDRM_eur,
       AVG(GRODDM_eur) as GRODDM_eur,
       AVG(CODDDM_eur) as CODDDM_eur,
       AVG(NGRDDM_eur) as NGRDDM_eur,
       AVG(MFCDDM_eur) as MFCDDM_eur,
       AVG(CRCCRA_eur) as CRCCRA_eur,
       AVG(CRCCRB_eur) as CRCCRB_eur,
       AVG(CRCCRC_eur) as CRCCRC_eur,
       AVG(CACPRG_eur) as CACPRG_eur,
       AVG(CACPRP_eur) as CACPRP_eur,
       AVG(CRCPMP_eur) as CRCPMP_eur,
       AVG(CRCPMM_eur) as CRCPMM_eur,
       AVG(MFCMKT_eur) as MFCMKT_eur,
       AVG(BWSMKT_eur) as BWSMKT_eur
FROM delta.finance_financial_reports_odp.pnl_order_level
WHERE order_id in (SELECT * FROM subway_orders)
GROUP BY 1
ORDER BY 1 DESC