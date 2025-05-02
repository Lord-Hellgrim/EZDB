#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <time.h>

#define DB_NAME "benchmark.db"
#define NUM_ROWS 10000

// Function to generate a random text starting with "text" + a number
void generate_text(char *buffer, size_t size, int id) {
    snprintf(buffer, size, "text%d_extra_data", rand() % 1000);
}

// Timing helper
double time_diff(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

int main() {
    sqlite3 *db;
    char *err_msg = 0;
    sqlite3_stmt *stmt;
    int rc;

    srand(time(NULL));

    // Open SQLite DB
    rc = sqlite3_open(DB_NAME, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    // Create table
    const char *create_sql = "DROP TABLE IF EXISTS test_table;"
                             "CREATE TABLE test_table (ints INTEGER, floats REAL, texts TEXT);";
    rc = sqlite3_exec(db, create_sql, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }

    // Insert data
    const char *insert_sql = "INSERT INTO test_table (ints, floats, texts) VALUES (?, ?, ?);";
    rc = sqlite3_prepare_v2(db, insert_sql, -1, &stmt, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare insert statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, NULL);
    for (int i = 0; i < NUM_ROWS; i++) {
        int i_val = rand() % 5;
        double f_val = (rand() % 2000) / 10.0;
        char text[50];
        generate_text(text, sizeof(text), i);

        sqlite3_bind_int(stmt, 1, i_val);
        sqlite3_bind_double(stmt, 2, f_val);
        sqlite3_bind_text(stmt, 3, text, -1, SQLITE_TRANSIENT);

        if (sqlite3_step(stmt) != SQLITE_DONE) {
            fprintf(stderr, "Insert failed: %s\n", sqlite3_errmsg(db));
        }
        sqlite3_reset(stmt);
    }
    sqlite3_exec(db, "END TRANSACTION;", NULL, NULL, NULL);
    sqlite3_finalize(stmt);

    // Time the SELECT query
    const char *query = "SELECT * FROM test_table WHERE ints = 1 OR floats > 10.0 OR texts LIKE 'text1%';";
    // const char *query = "SELECT * FROM test_table;";

    clock_t start, end;
    double cpu_time_used;
    
    start = clock();
    rc = sqlite3_prepare_v2(db, query, -1, &stmt, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare select statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    
    int counter = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        counter += 1;
        int i_val = sqlite3_column_int(stmt, 0);
        double f_val = sqlite3_column_double(stmt, 1);
        const unsigned char *text_val = sqlite3_column_text(stmt, 2);

        printf("Row: ints = %d, floats = %.2f, texts = %s\n", i_val, f_val, text_val);
    }
    
    printf("lines_processed: %d\n", counter);

    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    sqlite3_finalize(stmt);

    printf("Query took %.6f seconds\n", cpu_time_used);

    sqlite3_close(db);
    return 0;
}
