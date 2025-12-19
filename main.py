from etl.logger import setup_logging, get_stage_logger
from etl.extract import extract_data
from piplines.pipline import run_pipline
from piplines.create_ref_tables import create_ref_tables_con, create_ref_tables_ind

if __name__ == "__main__":
    run_pipline()
    #create_ref_tables_con()
    #create_ref_tables_ind()