<?php
namespace Phalcon\Db\Adapter\Pdo;
use Phalcon\Db\Column;
use Phalcon\Db\Result\PdoSqlsrv as ResultPdo;
/**
 * Phalcon\Db\Adapter\Pdo\Sqlsrv
 * Specific functions for the MsSQL database system
 * <code>
 * $config = array(
 * "host" => "192.168.0.11",
 * "db_name" => "blog",
 * "port" => 3306,
 * "username" => "sigma",
 * "password" => "secret"
 * );
 * $connection = new \Phalcon\Db\Adapter\Pdo\Sqlsrv($config);
 * </code>.
 *
 * @property \Phalcon\Db\Dialect\Sqlsrv $_dialect
 */
class Sqlsrv extends \Phalcon\Db\Adapter\Pdo implements \Phalcon\Db\AdapterInterface
{
    protected $_type = 'sqlsrv';
    protected $_dialectType = 'Sqlsrv';
    /**
     * This method is automatically called in Phalcon\Db\Adapter\Pdo constructor.
     * Call it when you need to restore a database connection.
     *
     * @param array $descriptor
     *
     * @return bool
     */
    public function connect(array $descriptor = null)
    {
        if (is_null($descriptor) === true) {
            $descriptor = $this->_descriptor;
        }
        /*
         * Check if the developer has defined custom options or create one from scratch
         */
        if (isset($descriptor['options']) === true) {
            $options = $descriptor['options'];
            unset($descriptor['options']);
        } else {
            $options = array();
        }
        $options[\PDO::ATTR_ERRMODE] = \PDO::ERRMODE_EXCEPTION;
        $options[\PDO::ATTR_STRINGIFY_FETCHES] = true;
        $this->_pdo = new \PDO("sqlsrv:server={$descriptor['host']};database={$descriptor['db_name']}", $descriptor['username'], $descriptor['password'], $options);
//        $this->execute('SET QUOTED_IDENTIFIER ON');
//        $this->execute("SET ANSI_WARNINGS ON ");
//        $this->execute("SET ANSI_NULLS ON ");
//        $this->execute("SET NOCOUNT ON ");
//        $this->execute("SET XACT_ABORT ON ");
        /*
         * Set dialect class
         */
        if (isset($descriptor['dialectClass']) === false) {
            $dialectClass = 'Phalcon\\Db\\Dialect\\' . $this->_dialectType;
        } else {
            $dialectClass = $descriptor['dialectClass'];
        }
        /*
         * Create the instance only if the dialect is a string
         */
        if (is_string($dialectClass) === true) {
            $dialectObject = new $dialectClass();
            $this->_dialect = $dialectObject;
        }
    }

    public function modifyColumn($tableName, $schemaName, \Phalcon\Db\ColumnInterface $column, \Phalcon\Db\ColumnInterface $currentColumn = null)
    {
        $this->execute($this->_dialect->modifyColumn($tableName,$schemaName,$column,$currentColumn));

        if($column->hasDefault() && !($currentColumn && $currentColumn->getDefault() === $column->getDefault()) ){
            $this->execute($this->_dialect->modifyColumnDefaultValue($tableName,$schemaName,$column,$currentColumn));
        }
    }

    public function dropPrimaryKey($tableName, $schemaName)
    {

        $name = $this->fetchColumn('SELECT name FROM sys.key_constraints WHERE type = \'PK\' and OBJECT_NAME(parent_object_id) = \''.$tableName.'\'');
        $this->execute("ALTER TABLE {$tableName} DROP CONSTRAINT {$name}");
    }


    public function describeIndexes($table, $schema = null)
    {

        $indexes = [];

        $query = $this->_dialect->describeIndexes($table,$schema);
        $temps = $this->fetchAll($query, \Phalcon\Db::FETCH_ASSOC);

        foreach ($temps as $index) {
            if($index['is_primary_key']){
                $indexes['PRIMARY']['columns'][] = $index['col_name'];
                $indexes['PRIMARY']['type'] = 'PRIMARY';
            }else if($index['is_unique']){
                $indexes[$index['index_name']]['columns'][] = $index['col_name'];
                $indexes[$index['index_name']]['type'] = 'UNIQUE';
            }else{
                $indexes[$index['index_name']]['columns'][] = $index['col_name'];
                $indexes[$index['index_name']]['type'] = null;
            }
        }
        $indexObjects = [];
        foreach ($indexes as $name => $data) {
            /**
             * Every index is abstracted using a Phalcon\Db\Index instance
             */
            $indexObjects[$name] = new \Phalcon\Db\Index($name, $data['columns'],$data['type']);
        }
        return $indexObjects;
    }

    public function describeReferences($table, $schema = null)
    {
        $indexes = [];

        $query = "exec sp_fkeys @fktable_name = '{$table}'";
        if ($schema) {
            $query .= ", @table_owner = '{$schema}'";
        }

        $temps = $this->fetchAll($query, \Phalcon\Db::FETCH_ASSOC);

        foreach ($temps as $index) {
            $indexes[$index['FK_NAME']]['referencedSchema']     = $index['PKTABLE_QUALIFIER'];
            $indexes[$index['FK_NAME']]['referencedTable']      = $index['PKTABLE_NAME'];
            $indexes[$index['FK_NAME']]['columns'][]            = $index['FKCOLUMN_NAME'];
            $indexes[$index['FK_NAME']]['referencedColumns'][]  = $index['PKCOLUMN_NAME'];
            $indexes[$index['FK_NAME']]['onUpdate']             = $this->castRule($index['UPDATE_RULE']);
            $indexes[$index['FK_NAME']]['onDelete']             = $this->castRule($index['DELETE_RULE']);

        }
        $indexObjects = [];
        foreach ($indexes as $name => $data) {
            /**
             * Every index is abstracted using a Phalcon\Db\Index instance
             */
            $indexObjects[$name] = new \Phalcon\Db\Reference($name,$data);// $data['columns'],$data['type']);
        }
        return $indexObjects;
    }

    private function castRule($rule){
        switch ($rule){
            case 1:
                return 'CASCADE';
            case 2:
                return 'NO ACTION';
        }
        return null;
    }


    /**
     * Returns an array of Phalcon\Db\Column objects describing a table
     * <code>
     * print_r($connection->describeColumns("posts"));
     * </code>.
     *
     * @param string $table
     * @param string $schema
     *
     * @return \Phalcon\Db\Column
     */
    public function describeColumns($table, $schema = null)
    {
        $oldColumn = null;
        /*
         * Get primary keys
         */
        $primaryKeys = array();
        foreach ($this->fetchAll($this->_dialect->getPrimaryKey($table, $schema)) as $field) {
            $primaryKeys[$field['COLUMN_NAME']] = true;
        }
        /*
         * Get the SQL to describe a table
         * We're using FETCH_NUM to fetch the columns
         * Get the describe
         * Field Indexes: 0:name, 1:type, 2:not null, 3:key, 4:default, 5:extra
         */
        foreach ($this->fetchAll($this->_dialect->describeColumns($table, $schema)) as $field) {
            /*
             * By default the bind types is two
             */
            $definition = array('bindType' => Column::BIND_PARAM_STR);
            /*
             * By checking every column type we convert it to a Phalcon\Db\Column
             */
            $autoIncrement = false;
            $columnType = $field['TYPE_NAME'];
            switch ($columnType) {
                /*
                 * Smallint/Bigint/Integers/Int are int
                 */
                case 'int identity':
                case 'tinyint identity':
                case 'smallint identity':
                    $definition['type'] = Column::TYPE_INTEGER;
                    $definition['isNumeric'] = true;
                    $definition['bindType'] = Column::BIND_PARAM_INT;
                    $definition['autoIncrement'] = true;
                    break;
                case 'bigint' :
                case 'bigint identity' :
                    $definition['type'] = Column::TYPE_BIGINTEGER;
                    $definition['isNumeric'] = true;
                    $definition['bindType'] = Column::BIND_PARAM_INT;
                    $definition['autoIncrement'] = true;
                    break;
                case 'decimal':
                case 'money':
                case 'smallmoney':
                    $definition['type'] = Column::TYPE_DECIMAL;
                    $definition['isNumeric'] = true;
                    $definition['bindType'] = Column::BIND_PARAM_DECIMAL;
                    break;
                case 'int':
                case 'tinyint':
                case 'smallint':
                    $definition['type'] = Column::TYPE_INTEGER;
                    $definition['isNumeric'] = true;
                    $definition['bindType'] = Column::BIND_PARAM_INT;
                    break;
                case 'numeric':
                    $definition['type'] = Column::TYPE_DOUBLE;
                    $definition['isNumeric'] = true;
                    $definition['bindType'] = Column::BIND_PARAM_DECIMAL;
                    break;
                case 'float':
                    $definition['type'] = Column::TYPE_FLOAT;
                    $definition['isNumeric'] = true;
                    $definition['bindType'] = Column::BIND_PARAM_DECIMAL;
                    break;
                /*
                 * Boolean
                 */
                case 'bit':
                    $definition['type'] = Column::TYPE_BOOLEAN;
                    $definition['bindType'] = Column::BIND_PARAM_BOOL;
                    break;
                /*
                 * Date are dates
                 */
                case 'date':
                    $definition['type'] = Column::TYPE_DATE;
                    break;
                /*
                 * Special type for datetime
                 */
                case 'datetime':
                case 'datetime2':
                case 'smalldatetime':
                    $definition['type'] = Column::TYPE_DATETIME;
                    break;
                /*
                 * Timestamp are dates
                 */
                case 'timestamp':
                    $definition['type'] = Column::TYPE_TIMESTAMP;
                    break;
                /*
                 * Chars are chars
                 */
                case 'char':
                case 'nchar':
                    $definition['type'] = Column::TYPE_CHAR;
                    break;
                case 'varchar':
                case 'nvarchar':
                    $definition['type'] = Column::TYPE_VARCHAR;
                    break;
                /*
                 * Text are varchars
                 */
                case 'text':
                case 'ntext':
                    $definition['type'] = Column::TYPE_VARCHAR;
                    break;
                /*
                 * blob type
                 */
                case 'varbinary':
                    $definition['type'] = Column::TYPE_BLOB;
                    break;
                /*
                 * By default is string
                 */
                case 'geography':
                    $definition['type'] = Column::TYPE_BLOB;
                    break;
                default:
                    var_dump($columnType);
                    $definition['type'] = Column::TYPE_VARCHAR;
                    break;
            }
            /*
             * If the column type has a parentheses we try to get the column size from it
             */
            $definition['size'] = (int) $field['LENGTH'];
            $definition['precision'] = (int) $field['PRECISION'];
            if ($field['SCALE'] || $field['SCALE'] == '0') {
                //                $definition["scale"] = (int) $field['SCALE'];
                $definition['size'] = $definition['precision'];
            }
            if($definition['type'] === Column::TYPE_TEXT){
                $definition['size'] = 'max';
            }

            /*
             * Positions
             */
            if (!$oldColumn) {
                $definition['first'] = true;
            } else {
                $definition['after'] = $oldColumn;
            }
            /*
             * Check if the field is primary key
             */
            if (isset($primaryKeys[$field['COLUMN_NAME']])) {
                $definition['primary'] = true;
            }
            /*
             * Check if the column allows null values
             */
            if ($field['NULLABLE'] == 0) {
                $definition['notNull'] = true;
            }
            /*
             * Check if the column is default values
             */
            if ($field['COLUMN_DEF'] != null) {
                $definition['default'] = $field['COLUMN_DEF'];
            }
            $columnName = $field['COLUMN_NAME'];
            $columns[] = new Column($columnName, $definition);
            $oldColumn = $columnName;
        }
        return $columns;
    }
    /**
     * Sends SQL statements to the database server returning the success state.
     * Use this method only when the SQL statement sent to the server is returning rows
     * <code>
     * //Querying data
     * $resultset = $connection->query("SELECTFROM robots WHERE type='mechanical'");
     * $resultset = $connection->query("SELECTFROM robots WHERE type=?", array("mechanical"));
     * </code>.
     *
     * @param string $sqlStatement
     * @param mixed  $bindParams
     * @param mixed  $bindTypes
     *
     * @return bool|\Phalcon\Db\ResultInterface
     */
    public function query($sqlStatement, $bindParams = null, $bindTypes = null)
    {
        $eventsManager = $this->_eventsManager;
        /*
         * Execute the beforeQuery event if a EventsManager is available
         */
        if (is_object($eventsManager)) {
            $this->_sqlStatement = $sqlStatement;
            $this->_sqlVariables = $bindParams;
            $this->_sqlBindTypes = $bindTypes;
            if ($eventsManager->fire('db:beforeQuery', $this, $bindParams) === false) {
                return false;
            }
        }
        $pdo = $this->_pdo;
        $cursor = \PDO::CURSOR_SCROLL;
        if (strpos($sqlStatement, 'exec') !== false) {
            $cursor = \PDO::CURSOR_FWDONLY;
        }
        if (is_array($bindParams)) {
            $statement = $pdo->prepare($sqlStatement, array(\PDO::ATTR_CURSOR => $cursor));
            if (is_object($statement)) {
                $statement = $this->executePrepared($statement, $bindParams, $bindTypes);
            }
        } else {
            $statement = $pdo->prepare($sqlStatement, array(\PDO::ATTR_CURSOR => $cursor));
            $statement->execute();
        }
        /*
         * Execute the afterQuery event if a EventsManager is available
         */

        if (is_object($statement)) {
            if (is_object($eventsManager)) {
                $eventsManager->fire('db:afterQuery', $this, $bindParams);
            }
            return new ResultPdo($this, $statement, $sqlStatement, $bindParams, $bindTypes);
        }
        return $statement;
    }
}