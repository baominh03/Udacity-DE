class SqlQueries:
    immg_table_insert = ("""
        SELECT 
                cic_id,
                i94_year,
                i94_mode,
                i94_port,
                i94_age,
                birth_year,
                gender,
                i94_address,
                i94_visa,
                visa_type,
                arrival_date,
                departure_date,
                sa.name as airport_name,
                sa.state as state_code,
                sc.state as state_name,
                sa.municipality as airport_city,
                sc.total_Population as total_population 
            FROM staging_immg si
            JOIN staging_airport sa
            ON si.i94_port = sa.iata_code
            JOIN staging_city sc
            ON sa.municipality = sc.city  
    """)

    air_table_insert = ("""
        SELECT *
        FROM staging_airport
    """)

    city_table_insert = ("""
        SELECT *
        FROM staging_city
    """)