from langchain_core.prompts import PromptTemplate

snowflake_prompt_template1 = PromptTemplate(
    input_variables=["schema_json"],
    template="""
  # SNOWFLAKE SCHEMA DESIGNER PROMPT

  ## ROLE: Expert Data Architect specializing in Snowflake dimensional modeling

  ## TASK: Convert relational schema to optimized Snowflake Schema (fact + dimensions)

  ## CORE RULES:
  1. **Fact Tables**:
    - Contain numeric measures + dimension FKs (SK_* only) only (Doesn't have *_ID fields)
    - Represent business events (transactions, sales, etc.)
    - Exclude audit/log tables unless essential
    - Replace dates with SK_DATE_ID (references D_DATE)
    - NOT INCLUDE self-referencing relationships

  2. **Dimension Tables**:
    - Include descriptive attributes, business keys (*_ID), and a numeric SK_* surrogate key.
    - SK_* is mandatory, used only as PK or FK to refer other table (never for business logic) and should be always NUMBER !!.
    - Always keep original *_ID as business key 
    - Use SK_* for all joins between dimensions or facts NEVER USE BUSINESS KEY *_ID(eg. SK_LOCATION)
    - For self-referencing relationships must be defined only using original business keys (e.g., MANAGER_ID referencing EMPLOYEE_ID) within the same dimension table. 
  3. **Structural Requirements**:
    - All tables get DT_INSERT (datetime)
    - Preserve the original column comments from the input schema, and adapt them only if the structure or naming of the column changes in the new Snowflake schema.
    - Include indexes (unique + foreign key hints)
    - Strict type inference

  ## KEY DECISIONS:
  - Fact table selection criteria (in order):
    1. Business event representation
    2. Numeric measures present
    3. Foreign key relationships
    4. Transactional naming patterns

  - Dimension normalization:
    - Split hierarchical attributes
    - Remove derived/redundant data
    - Add D_DATE dimension for all dates

  ## OUTPUT FORMAT (STRICT JSON):
  ```json
  {{
    "fact_table": {{
      "name": "F_...",
      "fields": {{...}},
      "comments": {{...}},
    }},
    "dimensions": [
      {{
        "name": "D_...",
        "fields": {{...}},
        "comments": {{...}},
      }},
      {{
        "name": "D_DATE",
        "fields": {{
          "SK_DATE_ID": "int",
          "DATE_VALUE": "date",
          "DAY": "int",
          "MONTH": "int",
          "YEAR": "int",
        }}
      }}
    ]
  }}
  ```

  ## PROCESS:
    1. Identify best fact table candidate
    2. Use all source table to extract dimensions with proper keys
    3. Normalize dimensions
    4. Add D_DATE dimension
    5. Apply indexes
    6. Adapt all original column comments from the input schema to align with the new data model, and include them in the final output.
    7. Validate against all rules

  ## PROHIBITED:
  - Explanations or commentary
  - Deviation from JSON format
  - Omission of required fields
  - Mixing business keys in fact tables

  INPUT SCHEMA:
  ```json
  {schema_json}
  ```

  OUTPUT ONLY THE JSON SCHEMA:
  """,
)


def get_main_agent_prompt():
    return """You are a database architect specialized in Oracle DDL generation. Your task is to:
    
        ## INSTRUCTIONS:
        1. Generate clean, organized Oracle DDL in this EXACT sequence:
            - CREATE TABLE statements first (all tables)
            - Then ALTER TABLE statements for constraints
            - Then CREATE INDEX statements
            - Finally COMMENT statements
        2. Follow Oracle best practices:
            - Use VARCHAR2 instead of VARCHAR
            - Use NUMBER for numeric fields
            - Explicitly specify NOT NULL where appropriate
            - Name all constraints properly
        3. Structure the output for maximum readability:
            - Blank line between each statement
            - Consistent indentation
            - Section headers as comments
        4. Include ALL elements from the schema:
            - All tables and columns
            - All relationships (primary/foreign keys)
            - All indexes
            - All comments

        ## RESPONSE FORMAT:
        
        Thought: What step should I take next?
        Action: The action to take (must be one of: {tool_names})
        Action Input: The input to the action (must be valid JSON)
        Observation: The result of the action
        ... (this Thought/Action/Action Input/Observation sequence can repeat N times)
        Thought: I have completed all steps and have the final DDL
        Final Answer: FINAL_DDL: [the complete Oracle DDL here]
        
        Begin!"""


def get_base_ddl_prompt2():
    return """Generate Oracle DDL for a snowflake schema data warehouse following these STRICT rules:

    ## TABLE CREATION RULES:
    - Use Oracle data types: VARCHAR2, NUMBER, DATE
    - Include all columns from the schema
    - Add DT_INSERT column to all tables if not present
    - For NUMBER columns, map Oracle precision/scale as follows:
      - If both precision and scale are provided, use `NUMBER(precision, scale)`.
      - If only precision is provided and scale is null, use `NUMBER(precision)`.
      - If both are null, use generic `NUMBER`.
    - For fact tables:
      * Include all foreign key columns
      * Mark measures with proper data types
    - For dimensions:
      * Include natural keys (*_ID)
      * All natural keys are UNIQUE.
      * Include all attributes
      * Handle self-referencing relationships

    ## CONSTRAINT NAMING RULES (MUST FOLLOW EXACTLY):
      - Max 30 characters (Oracle limit)
      - Use standard abbreviations:
        * EMP for EMPLOYEE
        * DEPT for DEPARTMENT
        * SAL for SALARY
        * etc.
      - Primary keys: PK_[TABLE_ABBR] (e.g. PK_F_EMP for F_EMPLOYEE)
      - Foreign keys: FK_[CHILD_ABBR]_[PARENT_ABBR] (e.g. FK_D_EMP_D_DEPT for EMPLOYEE→DEPARTMENT)
      - Unique constraints: UK_[TABLE_ABBR]_[COL_ABBR] (e.g. UK_D_EMP_EMAIL)
      - Check constraints: CK_[TABLE_ABBR]_[RULE_ABBR] (e.g. CK_EMP_SAL_GT_0)
      
     
    ## INDEX RULES:
    - Create indexes only for:
      * Foreign key columns
      * Columns marked as indexes in schema
      * Do NOT duplicate primary key indexes
  
    ## COMMENT RULES:
    - Include ALL original table and column comments
    - Format as: COMMENT ON TABLE/COLUMN [object] IS '[text]'
  
    ## OUTPUT STRUCTURE:
    1. First create ALL tables with columns but NO constraints (Just PRIMARY KEY Constraints)
    2. Then add constraints via ALTER TABLE statements (in Order : PRIMARY KEY , UNIQUE , FOREIGN KEY)
    3. Then create indexes not covered by constraints
    4. Finally add all comments

    ## FORMATTING:
    - One statement per line
    - Blank line between statements
    - Section headers as SQL comments (-- SECTION)"""


def get_validation_prompt():
    return """Analyze this Oracle DDL for syntax errors and provide specific feedback:

    Check for:
    1. Reserved word usage
    2. Proper data types
    3. Valid constraint syntax
    4. Correct reference to existing tables/columns
    5. Proper comment syntax

    Return ONLY the validation result in this format:
    VALIDATION: [PASSED|FAILED]
    ISSUES:
    - [issue 1]
    - [issue 2]"""


def get_optimization_prompt():
    return """Optimize this DDL for Oracle 19c with these enhancements:

    1. DATA TYPES:
       - VARCHAR2 instead of VARCHAR
       - NUMBER instead of INT/FLOAT
       - DATE for date fields
       - primary surrogate keys (SK_*) should be NUMBER 

    2. STORAGE:
       - Add TABLESPACE USERS for all tables
       - Add PCTFREE 10 PCTUSED 40

    3. CONSTRAINTS:
       - Named constraints (PK_, FK_, UK_ prefixes)

    4. ORGANIZATION:
       - Keep the original structure (tables -> constraints -> indexes -> sequence and triggers -> comments)
       - Maintain blank lines between statements"""


def get_constraints_prompt():
    return """Add all constraints to this DDL based on the schema:

    ## REQUIREMENTS:
    1. PRIMARY KEYS:
       - For all SK_ columns
       - Named as PK_[TABLE]
    
    2. FOREIGN KEYS:
       - For all relationships in schema
       - Named as FK_[CHILD]_[PARENT]
       - Proper ON DELETE rules

    3. UNIQUE CONSTRAINTS:
       - For natural keys (*_ID) in dimensions
       - Named as UK_[TABLE]_[COLUMN]

    4. NOT NULL:
       - For all required fields
       - SK_* Fields
       
    5. CHECK Constraints:
      - Add CHECK constraints if any business rules are present in column comments (e.g., "must be positive")
      - Name them as CK_[TABLE]_[COLUMN_RULE] (e.g., CK_SALARY_GT_0)

    6. SEQUENCES:
      - For each SK_ column in dimension tables, create a sequence to auto-increment the SK_ value.
      - The sequence should be created as follows:
        CREATE SEQUENCE SK_[TABLE]_SEQ
        START WITH 1
        INCREMENT BY 1
        NOCACHE
        NOCYCLE;
      - Ensure the sequence is used for auto-incrementing SK_ values via triggers.

    ## OUTPUT FORMAT:
    -- CONSTRAINTS FOR [TABLE]
    ALTER TABLE [TABLE] ADD CONSTRAINT [NAME] ...;
    CREATE SEQUENCE SK_[TABLE]_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
    [blank line]"""


def get_base_ddl_prompt1():
    return """You are an expert Oracle SQL engineer. Generate **production-ready Oracle DDL statements** to build a **Snowflake schema data warehouse**. Follow these rules with **strict precision**:

  --============================================================--
  --                  TABLE CREATION RULES                      --
  --============================================================--
  - Use **only** Oracle data types: `VARCHAR2`, `NUMBER`, `DATE`.
  - Include **all columns** defined in the schema.
  - Add a `DT_INSERT` column of type `DATE` to **every table** if it’s not already present.
  - FACT tables:
    * Must include **all foreign key columns**.
    * Measures must have accurate numeric data types.
  - DIMENSION tables:
    * Must include **natural keys** (named `*_ID`).
    * All natural keys must be **UNIQUE**.
    * Include **all descriptive attributes**.
    * Support self-referencing relationships (e.g. `MANAGER_ID` referencing `EMPLOYEE_ID`).

  --============================================================--
  --                CONSTRAINT NAMING CONVENTIONS              --
  --============================================================--
  - Max length: 30 characters (Oracle limit).
  - Use these **abbreviations** for naming:
    * EMP for EMPLOYEE, DEPT for DEPARTMENT, SAL for SALARY, etc.
  - Naming patterns:
    * Primary Keys: `PK_<TABLE_ABBR>` → ex: `PK_F_EMP`
    * Foreign Keys: `FK_<CHILD_ABBR>_<PARENT_ABBR>` → ex: `FK_D_EMP_D_DEPT`
    * Unique: `UK_<TABLE_ABBR>_<COL_ABBR>` → ex: `UK_D_EMP_EMAIL`
    * Check: `CK_<TABLE_ABBR>_<RULE_ABBR>` → ex: `CK_EMP_SAL_GT_0`

  --============================================================--
  --                     INDEX RULES                            --
  --============================================================--
  - Create indexes **only** on:
    * Foreign key columns
    * Columns explicitly marked for indexing in the schema
  - Do **NOT** duplicate primary key indexes.

  --============================================================--
  --                    SEQUENCE RULES                          --
  --============================================================--
  - For each `SK_` column in dimension tables, create an Oracle sequence:
    ```
    CREATE SEQUENCE SK_<TABLE>_SEQ
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;
    ```
  - Use this sequence with a `BEFORE INSERT` trigger to auto-increment the surrogate key.

  --============================================================--
  --                     COMMENT RULES                          --
  --============================================================--
  - Preserve **all table and column descriptions**.
  - Format:
    * `COMMENT ON COLUMN <table_name>.<column_name> IS '<text>';`

  --============================================================--
  --                  OUTPUT STRUCTURE                          --
  --============================================================--
  1. First: Create **all tables** with columns and primary key constraints only.
  2. Second: Add **all other constraints** using `ALTER TABLE`:
    - Primary Keys (if not added above)
    - Unique Constraints
    - Foreign Keys
  3. Third: Create **indexes** not covered by constraints.
  4. Fourth: Create **sequences and triggers** for surrogate keys.
  5. Finally: Add **all comments** for tables and columns.

  --============================================================--
  --                      FORMATTING RULES                      --
  --============================================================--
  - One SQL statement per line.
  - Insert a blank line between SQL statements.
  - Use clear section headers as SQL comments:
    * e.g. `-- TABLE DEFINITIONS`, `-- CONSTRAINTS`, `-- INDEXES`,`-- SEQUENCES AND TRIGGERS `,`-- COMMENTS`
"""


def get_base_ddl_prompt():
    return """Generate Oracle 11g DDL for a snowflake schema data warehouse following these STRICT rules:

    ## TABLE CREATION RULES:
    - Use Oracle data types: VARCHAR2, NUMBER, DATE 
    - Include all columns from the schema
    - Add DT_INSERT column to all tables if not present
    - For fact tables:
      * Include all foreign key columns
      * Mark measures with proper data types
      * All SK_* should be NOT NULL and **NUMBER TYPE** 
      * Each SK_* column must reference its corresponding dimension table via a FOREIGN KEY constraint
      * The **primary key of the fact table must be a composite key** composed of **all the `SK_*` foreign keys** (i.e., all dimension references)
    - For dimensions:
      * Include natural keys (*_ID) 
      * All natural keys are UNIQUE.
      * Include all attributes
      - Must include SK_* surrogate key (SHOULD BE NUMBER TYPE)
      * Add Foreign key contraint to handle self-referencing relationships to map with the business key (*_ID) (eg. MANAGER_ID -> EMPLOYEE_ID)
    - Do NOT use GENERATED AS IDENTITY (not supported in Oracle 11g)
    - For all surrogate key columns prefixed with SK_*, generate:
      * A SEQUENCE named SEQ_[COLUMN_NAME] (e.g., SEQ_SK_EMPLOYEE_ID)
      * A BEFORE INSERT trigger on the corresponding table
        - Trigger should assign NEXTVAL from the sequence to the SK_* column if its value is NULL

    ## CONSTRAINT NAMING RULES (MUST FOLLOW EXACTLY):
      - Max 30 characters (Oracle limit)
      - Use standard abbreviations:
        * EMP for EMPLOYEE
        * DEPT for DEPARTMENT
        * SAL for SALARY
        * etc.
      - Primary keys: PK_[TABLE_ABBR] (e.g. PK_F_EMP for F_EMPLOYEE)
      - Foreign keys: FK_[CHILD_ABBR]_[PARENT_ABBR] (e.g. FK_D_EMP_D_DEPT for EMPLOYEE→DEPARTMENT) NB: Don't Forget Self referencing if exist 
      - Unique constraints: UK_[TABLE_ABBR]_[COL_ABBR] (e.g. UK_D_EMP_EMAIL)
      - Check constraints: CK_[TABLE_ABBR]_[RULE_ABBR] (e.g. CK_EMP_SAL_GT_0)

    ## INDEX RULES:
    - Create indexes only for:
      * Foreign key columns (SK_*)
      * Do NOT duplicate primary key indexes

    ## COMMENT RULES:
    - Include ALL original table and column comments
    - Format as: COMMENT ON TABLE/COLUMN [object] IS '[text]'

    ## OUTPUT STRUCTURE:
    1. First create ALL tables with columns and PRIMARY KEY constraints only
    2. Then add other constraints via ALTER TABLE (in Order: PRIMARY KEY, UNIQUE, FOREIGN KEY)
      *Use clear and consistent constraint names (e.g., PK_, UQ_, FK_ prefixes).
    3. Then for each SK_* column in the Dimensions tables:
       - Create the SEQUENCE as: CREATE SEQUENCE SEQ_[COLUMN_NAME] START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
       - Create the TRIGGER as (RESPECT THE IDENTATION):
       ```
        CREATE OR REPLACE TRIGGER TRG_[TABLE_NAME]
        BEFORE INSERT ON [TABLE_NAME]
        FOR EACH ROW
        WHEN (NEW.[SK_COLUMN] IS NULL)
        BEGIN
          SELECT SEQ_[COLUMN_NAME].NEXTVAL
          INTO :NEW.[SK_COLUMN]
          FROM dual;
        END;
        /
        ```
       - Replace [COLUMN_NAME], [TABLE_NAME], and [SK_COLUMN] with actual names.
    4. Then create indexes not covered by constraints
    5. Finally add all comments

    ## FORMATTING:
    - One statement per line
    - Blank line between statements
    - Section headers as SQL comments (-- SECTION)
"""


snowflake_prompt_template = PromptTemplate(
    input_variables=["schema_json"],
    template="""
  # SNOWFLAKE SCHEMA DESIGNER PROMPT

  ## ROLE: Expert Data Architect specializing in Snowflake dimensional modeling

  ## TASK: Convert relational schema to optimized Snowflake Schema (fact + dimensions)

  ## CORE RULES:
  1. **Fact Tables**:
    - Contain numeric measures + dimension FKs (SK_* only) only (Doesn't have *_ID fields)
    - Represent business events (transactions, sales, etc.)
    - Exclude audit/log tables unless essential
    - Replace dates with SK_DATE_ID (references D_DATE)
    - NOT INCLUDE self-referencing relationships

  2. **Dimension Tables**:
    - Include descriptive attributes, business keys (*_ID), and a numeric SK_* surrogate key.
    - SK_* is mandatory, used only as PK or FK to refer other table (never for business logic) and should be always NUMBER !!.
    - Always keep original *_ID as business key 
    - Use SK_* for all joins between dimensions or facts NEVER USE BUSINESS KEY *_ID(eg. SK_LOCATION)
    - For self-referencing relationships must be defined only using original business keys (e.g., MANAGER_ID referencing EMPLOYEE_ID) within the same dimension table. 
  3. **Structural Requirements**:
    - All tables get DT_INSERT (datetime)
    - Preserve the original column comments from the input schema, and adapt them only if the structure or naming of the column changes in the new Snowflake schema.
    - Include indexes (unique + foreign key hints)
    - Strict type inference

  ## KEY DECISIONS:
  - Fact table selection criteria (in order):
    1. Business event representation
    2. Numeric measures present
    3. Foreign key relationships
    4. Transactional naming patterns

  - Dimension normalization:
    - Split hierarchical attributes
    - Remove derived/redundant data
    - Add D_DATE dimension for all dates

  ## OUTPUT FORMAT (STRICT JSON):
  ```json
  {{
    "fact_table": {{
      "name": "F_...",
      "fields": {{...}},
      "comments": {{...}},
    }},
    "dimensions": [
      {{
        "name": "D_...",
        "fields": {{...}},
        "comments": {{...}},
      }},
      {{
        "name": "D_DATE",
        "fields": {{
          "SK_DATE_ID": "int",
          "DATE_VALUE": "date",
          "DAY": "int",
          "MONTH": "int",
          "YEAR": "int",
        }}
      }}
    ]
  }}
  ```

  ## PROCESS:
    1. Identify best fact table candidate
    2. Use all source table to extract dimensions with proper keys
    3. Normalize dimensions
    4. Add D_DATE dimension
    5. Apply indexes
    6. Adapt all original column comments from the input schema to align with the new data model, and include them in the final output.
    7. Validate against all rules

  ## PROHIBITED:
  - Explanations or commentary
  - Deviation from JSON format
  - Omission of required fields
  - Mixing business keys in fact tables

  INPUT SCHEMA:
  ```json
  {schema_json}
  ```

  OUTPUT ONLY THE JSON SCHEMA:
  """,
)
