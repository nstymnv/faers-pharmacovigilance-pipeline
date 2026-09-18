from config import build_sf_options, RAW_DATA_DIR
from extract.scanner import extract_data
from extract.spark import create_spark
from load.loader import write_to_db
from load.parser import (
    extract_demographics,
    extract_drug,
    extract_reaction,
    extract_reports,
)


def main() -> None:
    spark = create_spark()
    sf_options = build_sf_options()

    raw_data = extract_data(spark, str(RAW_DATA_DIR / "XML" / "*.xml"))

    df_reports = extract_reports(raw_data)
    df_demographics = extract_demographics(raw_data)
    df_drug = extract_drug(raw_data)
    df_reaction = extract_reaction(raw_data)

    write_to_db(df_reports, "REPORTS", sf_options)
    write_to_db(df_demographics, "DEMOGRAPHICS", sf_options)
    write_to_db(df_drug, "DRUG", sf_options)
    write_to_db(df_reaction, "REACTION", sf_options)


if __name__ == "__main__":
    main()
