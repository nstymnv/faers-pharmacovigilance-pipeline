from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, explode_outer, posexplode_outer, row_number


def extract_reports(df: DataFrame) -> DataFrame:
    return df.select(
        col("safetyreportid"),
        col("safetyreportversion"),
        col("receiptdate"),
        col("transmissiondate"),
        col("primarysourcecountry"),
        col("occurcountry"),
        col("reporttype"),
        col("serious"),
        col("seriousnesscongenitalanomali"),
        col("seriousnessdeath"),
        col("seriousnessdisabling"),
        col("seriousnesshospitalization"),
        col("seriousnesslifethreatening"),
        col("seriousnessother"),
        col("fulfillexpeditecriteria"),
        col("duplicate"),
        col("reportduplicate.duplicatenumb").alias("duplicatenumb"),
        col("reportduplicate.duplicatesource").alias("duplicatesource"),
        col("authoritynumb"),
        col("companynumb"),
    )


def extract_demographics(df: DataFrame) -> DataFrame:
    return df.select(
        col("safetyreportid"),
        col("safetyreportversion"),
        col("patient.patientagegroup").alias("patientagegroup"),
        col("patient.patientonsetage").alias("patientonsetage"),
        col("patient.patientonsetageunit").alias("patientonsetageunit"),
        col("patient.patientsex").alias("patientsex"),
        col("patient.patientweight").alias("patientweight"),
    )


def extract_drug(df: DataFrame) -> DataFrame:
    df_drug_exploded = df.select(
        col("safetyreportid"),
        col("safetyreportversion"),
        explode_outer(col("patient.drug")).alias("drug"),
    )

    df_drug_flat = df_drug_exploded.select(
        col("safetyreportid"),
        col("safetyreportversion"),
        col("drug.medicinalproduct").alias("medicinalproduct"),
        col("drug.activesubstance.activesubstancename").alias("activesubstancename"),
        col("drug.drugcharacterization").alias("drugcharacterization"),
        col("drug.drugindication").alias("drugindication"),
        col("drug.drugbatchnumb").alias("drugbatchnumb"),
        col("drug.drugauthorizationnumb").alias("drugauthorizationnumb"),
        col("drug.drugadministrationroute").alias("drugadministrationroute"),
        col("drug.drugstructuredosagenumb").alias("drugstructuredosagenumb"),
        col("drug.drugstructuredosageunit").alias("drugstructuredosageunit"),
        col("drug.drugstartdate").alias("drugstartdate"),
        col("drug.drugenddate").alias("drugenddate"),
        col("drug.drugtreatmentduration").alias("drugtreatmentduration"),
        col("drug.drugtreatmentdurationunit").alias("drugtreatmentdurationunit"),
        col("drug.drugrecurreadministration").alias("drugrecurreadministration"),
        col("drug.actiondrug").alias("actiondrug"),
        col("drug.drugadditional").alias("drugadditional"),
        posexplode_outer(col("drug.drugrecurrence")).alias("recurrence_index", "drugrecurrence"),
    )

    df_recurrence_exploded = df_drug_flat.select(
        "*",
        col("drugrecurrence.drugrecuraction").alias("drugrecuraction"),
    ).drop("drugrecurrence")

    # Exploding drugrecurrence fans out one row per recurrence event for the
    # same drug. Collapse back to one row per drug, keeping the last recurrence
    # entry that has a valid (non-null) recurrence action.
    drug_key = [
        c for c in df_recurrence_exploded.columns
        if c not in ("recurrence_index", "drugrecuraction")
    ]
    last_valid_recurrence = Window.partitionBy(*drug_key).orderBy(
        col("drugrecuraction").isNull().asc(),
        col("recurrence_index").desc(),
    )

    return (
        df_recurrence_exploded.withColumn("rn", row_number().over(last_valid_recurrence))
        .where(col("rn") == 1)
        .drop("recurrence_index", "rn")
    )


def extract_reaction(df: DataFrame) -> DataFrame:
    df_reaction_exploded = df.select(
        col("safetyreportid"),
        col("safetyreportversion"),
        explode_outer(col("patient.reaction")).alias("reaction"),
    )

    return df_reaction_exploded.select(
        col("safetyreportid"),
        col("safetyreportversion"),
        col("reaction.reactionmeddrapt").alias("reactionmeddrapt"),
        col("reaction.reactionmeddraversionpt").alias("reactionmeddraversionpt"),
        col("reaction.reactionoutcome").alias("reactionoutcome"),
    )
