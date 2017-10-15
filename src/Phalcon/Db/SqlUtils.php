<?php

namespace Phalcon\Db;


trait SqlUtils
{
    public function addComment($table,$comment,$columns = [])
    {
        $comment = mb_convert_encoding($comment,"utf8");
        self::$_connection->execute(
            "IF exists(SELECT 1 FROM fn_listextendedproperty('MS_Description', 'SCHEMA', 'dbo', 'TABLE', '${table}', NULL, NULL) WHERE name = 'MS_Description')"
            ."EXEC sp_dropextendedproperty 'MS_Description', 'SCHEMA', 'dbo', 'TABLE', '${table}'\n"
            ."EXEC sp_addextendedproperty 'MS_Description', '${comment}', 'SCHEMA', 'dbo', 'TABLE', '${table}'"
        );
        foreach ($columns as $column => $comment){
            $comment = mb_convert_encoding($comment,"utf8");
            self::$_connection->execute(
                "IF exists(SELECT 1 FROM fn_listextendedproperty('MS_Description', 'SCHEMA', 'dbo', 'TABLE', '{$table}', 'COLUMN', '${column}') WHERE name = 'MS_Description')"
                ."EXEC sp_dropextendedproperty 'MS_Description', 'SCHEMA', 'dbo', 'TABLE', '${table}', 'COLUMN', '${column}'\n"
                ."EXEC sp_addextendedproperty 'MS_Description', '${comment}', 'SCHEMA', 'dbo', 'TABLE', '${table}', 'COLUMN', '${column}'"
            );
        }
    }
}
