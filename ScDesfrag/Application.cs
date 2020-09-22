using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Text;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Text.RegularExpressions;

public class Application
{
  private readonly ILogger _logger;
  private string _connString;
  private int _debug;
  private int _minFrag;
  private int _minPag;
  private int _maxPag;
  private string _schema;
  private string _tableName;
  private bool _canRun;

  public Application(ILogger<Application> logger)
  {
    _logger = logger;

    // Pega Connection String de appsettings.json e armazena em ConnString
    var builder = new ConfigurationBuilder().AddJsonFile("appsettings.json");

    var configuration = builder.Build();
    _connString = configuration.GetConnectionString("DataConnection");
  }

  internal void Run(string[] args)
  {
    // Carrega parametros
    LoadParams(args);

    if (_canRun)
    {

      _logger.LogInformation("---------------------------------------------------------------------------------");
      _logger.LogInformation("DESFRAGMENTAÇÃO DE TABELAS DO BANCO DE DADOS DO X3");
      _logger.LogInformation("INICIO: “{0}”", DateTime.UtcNow);
      _logger.LogInformation("---------------------------------------------------------------------------------");

      _logger.LogInformation("Parâmetros: ");
      _logger.LogInformation("  Valor mínimo de fragmentação: " + _minFrag.ToString());
      _logger.LogInformation("  Valor mínimo de paginação: " + _minPag.ToString());
      _logger.LogInformation("  Valor máximo de paginação: " + _maxPag.ToString());
      _logger.LogInformation("  Nome do schema: " + _schema);
      _logger.LogInformation("  Nome da tabela: " + _tableName);
      _logger.LogInformation("---------------------------------------------------------------------------------");

      // Executa tarefa principal
      MainTask();

      _logger.LogInformation("---------------------------------------------------------------------------------");
      _logger.LogInformation("DESFRAGMENTAÇÃO DE TABELAS DO BANCO DE DADOS DO X3,");
      _logger.LogInformation("TÉRMINO: “{0}“", DateTime.UtcNow);
      _logger.LogInformation("---------------------------------------------------------------------------------\n");
    }
    else
    {
      Console.WriteLine("Por favor, execute via ScopeDesfragmentacao.exe");
    }
  }

  private void LoadParams(string[] args)
  {

    // 0 = Não informa logs de debug
    // 1 = Informa logs de debug
    _debug = 0;

    // Número mínimo de fragmentos da tabela para ser desfragmentada
    // Default = 0
    _minFrag = 10;

    // Número mínimo de páginas da tabela para ser desfragmentada
    // Default = 1
    _minPag = 1000;

    // Número máximo de páginas da tabela para ser desfragmentada
    // Default = -1 (sem limite)
    _maxPag = -1;

    // Nome do Schema do banco de dados a ser processado
    // Se o valor desse parâmetro for vazio, seleciona todos os schemas
    // Default = ""
    _schema = String.Empty;

    // Nome da tabela a ser desfragmentada
    // Se o valor desse parâmetro não for uma string vazia, todos os outros parâmetros são descartados.
    // Default = ""
    _tableName = String.Empty;

    // Programa só pode ser executado via ScopeDesfragmentacao.exe
    // Isso é feito sendo obrigatório o parâmetro --secret-key-execute
    _canRun = false;

    int debugIndex = Array.IndexOf(args, "--debug");
    int minFragIndex = Array.IndexOf(args, "--min-frag");
    int minPagIndex = Array.IndexOf(args, "--min-pag");
    int maxPagIndex = Array.IndexOf(args, "--max-pag");
    int schemaIndex = Array.IndexOf(args, "--schema");
    int tableNameIndex = Array.IndexOf(args, "--table");
    int canRun = Array.IndexOf(args, "--secret-key-execute");

    if (minFragIndex >= 0)
      _minFrag = int.Parse(args[minFragIndex + 1]);

    if (minPagIndex >= 0)
      _minPag = int.Parse(args[minPagIndex + 1]);

    if (maxPagIndex >= 0)
      _maxPag = int.Parse(args[maxPagIndex + 1]);

    if (schemaIndex >= 0)
      _schema = args[schemaIndex + 1];

    if (tableNameIndex >= 0)
      _tableName = args[tableNameIndex + 1];

    if (debugIndex >= 0)
      _debug = 1;

    if (canRun >= 0)
      _canRun = true;
  }

  private void MainTask()
  {
    try
    {
      // Monta select no banco para selecionar todas as tabelas ordenadas pela fragmentação desc
      StringBuilder sb = new StringBuilder();


      if (String.IsNullOrEmpty(_tableName))
      {
        // Se não passar o nome da tabela usa os outros parâmetros
        sb.Append("SELECT dbschemas.[name] as 'Schema',\n");
        sb.Append("dbtables.[name] as 'Table',\n");
        sb.Append("indexstats.page_count,\n");
        sb.Append("indexstats.avg_fragmentation_in_percent\n");
        sb.Append("FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS indexstats\n");
        sb.Append("INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]\n");
        sb.Append("INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]\n");
        sb.Append("INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]\n");
        sb.Append("AND indexstats.index_id = dbindexes.index_id\n");
        sb.Append("WHERE indexstats.database_id = DB_ID() AND indexstats.page_count >= " + _minPag.ToString() + " \n");

        if (_maxPag > 0)
          sb.Append("AND indexstats.page_count <= " + _maxPag.ToString() + " \n");

        if (!(String.IsNullOrEmpty(_schema)))
          sb.Append("AND UPPER(dbschemas.[name]) like '" + _schema.ToUpper() + "' \n");


        sb.Append("GROUP BY dbschemas.[name], dbtables.[name], indexstats.page_count, indexstats.avg_fragmentation_in_percent\n");
        sb.Append("ORDER BY indexstats.page_count desc\n");
      }
      else
      {
        // Filtra pelo nome da tabela

        sb.Append("SELECT dbschemas.[name] as 'Schema',\n");
        sb.Append("dbtables.[name] as 'Table',\n");
        sb.Append("indexstats.page_count,\n");
        sb.Append("indexstats.avg_fragmentation_in_percent\n");
        sb.Append("FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS indexstats\n");
        sb.Append("INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]\n");
        sb.Append("INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]\n");
        sb.Append("INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]\n");
        sb.Append("AND indexstats.index_id = dbindexes.index_id\n");
        sb.Append("WHERE indexstats.database_id = DB_ID() AND UPPER(dbtables.[name]) = '" + _tableName.ToUpper() + "'\n");

        if (!String.IsNullOrEmpty(_schema))
          sb.Append("AND UPPER(dbschemas.[name]) like '" + _schema.ToUpper() + "' \n");

        sb.Append("GROUP BY dbschemas.[name], dbtables.[name], indexstats.page_count, indexstats.avg_fragmentation_in_percent\n");
        sb.Append("ORDER BY indexstats.page_count desc\n");
      }

      string sql = sb.ToString();

      logDebug("Executando select c/ todas as tabelas e fragmentação");

      int TotalLinhas = 0, TotalLinhasFragmentadas = 0, TotalLinhasDesfragmentadasSucesso = 0;

      var dsTables = ExecSql(sql);

      _logger.LogInformation("Iniciando desfragmentação...");
      _logger.LogInformation("---------------------------------------------------------------------------------");


      foreach (DataRow table in dsTables.Tables[0].Rows)
      {
        try
        {
          TotalLinhas++;

          //Thread.Sleep(1000);

          string tableName = table["Schema"].ToString() + "." + table["Table"].ToString();
          decimal avgFrag = Convert.ToDecimal(table["avg_fragmentation_in_percent"].ToString());

          logDebug("---------------------------------------------------------------------------------");
          logDebug("Processando tabela: \"" + tableName + "\", percentual de fragmentação: " + avgFrag.ToString());

          if (avgFrag >= _minFrag)
          {

            // Verifica se a tabela está com algum lock
            VerifyLock(tableName);

            // Verifica se a tabela possui algum index cluster
            bool hasClusteredIndex = checkClusteredIndex(tableName);

            if (hasClusteredIndex)
            {
              logDebug("Tabela possúi index cluster");

              if (RecreateExistingIndex(tableName))
                TotalLinhasDesfragmentadasSucesso++;
            }
            else
            {
              logDebug("Tabela não possúi index cluster");

              if (CreateAndDropIndex(tableName))
                TotalLinhasDesfragmentadasSucesso++;
            }

            TotalLinhasFragmentadas++;
          }
          else
          {
            logDebug("Percentual de fragmentação < " + _minFrag.ToString() + ", ignorando");
          }
        }
        catch (Exception e)
        {
          _logger.LogError(e.ToString());
        }

      }

      _logger.LogInformation("---------------------------------------------------------------------------------");
      _logger.LogInformation("Total de tabelas executadas: {0}", TotalLinhas);
      _logger.LogInformation("Total de tabelas com fragmentação >= {0}: {1}", _minFrag, TotalLinhasFragmentadas);
      _logger.LogInformation("Total de tabelas desfragmentadas com sucesso: {0}", TotalLinhasDesfragmentadasSucesso);
      _logger.LogInformation("Total erros ao desfragmentar: {0}", TotalLinhasFragmentadas - TotalLinhasDesfragmentadasSucesso);

    }
    catch (Exception e)
    {
      _logger.LogError(e.ToString());
    }
  }

  private bool CreateAndDropIndex(string tableName)
  {
    logDebug("Selecionando índices da tabela");

    string getIndexSql = "EXEC sys.sp_helpindex @objname = N'" + tableName + "'";

    var dsIndex = ExecSql(getIndexSql);

    string selectedIndex = "";
    string selectedColumns = "";

    foreach (DataRow index in dsIndex.Tables[0].Rows)
    {
      if (selectedIndex == "")
      {
        selectedIndex = index["index_name"].ToString();
        selectedColumns = index["index_keys"].ToString();
      }
      else
      {
        if (selectedIndex.IndexOf("ROWID") >= 0)
        {
          selectedIndex = index["index_name"].ToString();
          selectedColumns = index["index_keys"].ToString();
        }
      }

      logDebug("Índice: " + index["index_name"].ToString());
    }

    selectedColumns = Regex.Replace(selectedColumns, @"\(-\)", " DESC");

    logDebug("Índice utilizado como base: " + selectedIndex);
    logDebug("Colunas: " + selectedColumns);

    string dataConvertida = DateTime.Now.Year.ToString() + DateTime.Now.Month.ToString() + DateTime.Now.Day.ToString() + DateTime.Now.Hour.ToString() + DateTime.Now.Minute.ToString();
    string newIndexName = "CLUSTEREDINDEX_" + dataConvertida;

    // Aqui começa a transação

    using (var conn = new SqlConnection(_connString))
    {
      logDebug("Abrindo conexão");
      conn.Open();

      SqlCommand command = conn.CreateCommand();
      SqlTransaction transaction = conn.BeginTransaction(IsolationLevel.Serializable, "ScCDI-" + newIndexName);

      command.Connection = conn;
      command.Transaction = transaction;

      try
      {
        logDebug("Criando índice cluster");

        StringBuilder createIndexSql = new StringBuilder();

        createIndexSql.Append("CREATE CLUSTERED INDEX " + newIndexName);
        createIndexSql.Append("\nON " + tableName + "(" + selectedColumns + ")");

        logDebug(createIndexSql.ToString());
        command.CommandText = createIndexSql.ToString();
        command.ExecuteNonQuery();

        logDebug("Índice criado");

        logDebug("Dropando índice cluster");

        StringBuilder dropIndexSql = new StringBuilder();

        dropIndexSql.Append("DROP INDEX " + newIndexName + " ON " + tableName);

        logDebug(dropIndexSql.ToString());

        command.CommandText = dropIndexSql.ToString();
        command.ExecuteNonQuery();

        logDebug("Indice dropado");

        logDebug("Comitando transação");
        // Attempt to commit the transaction.
        transaction.Commit();
        logDebug("Índice cluster derrubado com sucesso.");

        _logger.LogInformation("Tabela: " + tableName + " Desfragmentada");

        return true;
      }
      catch (Exception e)
      {
        try
        {
          logDebug("Falha durante a transação, iniciando Rollback.");
          _logger.LogError(e.ToString());
          transaction.Rollback();
          return false;
        }
        catch (Exception ex2)
        {
          // This catch block will handle any errors that may have occurred
          // on the server that would cause the rollback to fail, such as
          // a closed connection.
          _logger.LogError("Rollback Exception Type: " + ex2.GetType());
          _logger.LogError("Message: " + ex2.Message);
          return false;
        }
      }
    }

    // Aqui termina a transação

  }

  private bool RecreateExistingIndex(string tableName)
  {
    try
    {
      logDebug("Select pegar nome do index cluster");

      StringBuilder sqlIndexCluster = new StringBuilder();

      sqlIndexCluster.Append("SELECT [name] FROM sys.indexes\n");
      sqlIndexCluster.Append("WHERE object_id = OBJECT_ID('" + tableName + "') AND index_id = 1\n");

      DataSet dsIndexClusterName = ExecSql(sqlIndexCluster.ToString());

      string indexClusterName = dsIndexClusterName.Tables[0].Rows[0]["name"].ToString();

      logDebug("Index cluster da tabela: " + indexClusterName);

      string getIndexesSql = "EXEC sys.sp_helpindex @objname = N'" + tableName + "'";

      DataSet dsIndexes = ExecSql(getIndexesSql);

      string currentIndexName = "";
      string currentIndexColumns = "";

      foreach (DataRow index in dsIndexes.Tables[0].Rows)
      {
        if (indexClusterName == index["index_name"].ToString())
        {
          currentIndexName = index["index_name"].ToString();
          currentIndexColumns = index["index_keys"].ToString();
        }
      }

      currentIndexColumns = Regex.Replace(currentIndexColumns, @"\(-\)", " DESC");

      logDebug("Índice Cluster: " + currentIndexName);
      logDebug("Colunas: " + currentIndexColumns);


      // Aqui começa a transação

      using (var conn = new SqlConnection(_connString))
      {
        logDebug("Abrindo conexão");
        conn.Open();

        SqlCommand command = conn.CreateCommand();
        SqlTransaction transaction = conn.BeginTransaction(IsolationLevel.Serializable, "ScREI-" + currentIndexName);

        command.Connection = conn;
        command.Transaction = transaction;

        try
        {
          logDebug("Dropando índice cluster");
          string dropIndexSql = "DROP INDEX " + currentIndexName + " ON " + tableName;
          logDebug(dropIndexSql);
          command.CommandText = dropIndexSql;
          command.ExecuteNonQuery();

          logDebug("Índice dropado");
          logDebug("Recriando índice cluster");

          StringBuilder createIndexSql = new StringBuilder();

          createIndexSql.Append("CREATE CLUSTERED INDEX " + currentIndexName);
          createIndexSql.Append("\nON " + tableName + "(" + currentIndexColumns + ")");

          logDebug(createIndexSql.ToString());

          command.CommandText = createIndexSql.ToString();
          command.ExecuteNonQuery();
          logDebug("Índice criado");

          logDebug("Comitando transação");
          // Attempt to commit the transaction.
          transaction.Commit();
          logDebug("Índice criado");

          _logger.LogInformation("Tabela: " + tableName + " Desfragmentada");
          return true;
        }
        catch (Exception e)
        {
          try
          {
            logDebug("Falha durante a transação, iniciando Rollback.");
            _logger.LogError(e.ToString());
            transaction.Rollback();
            return false;
          }
          catch (Exception ex2)
          {
            // This catch block will handle any errors that may have occurred
            // on the server that would cause the rollback to fail, such as
            // a closed connection.
            _logger.LogError("Rollback Exception Type: " + ex2.GetType());
            _logger.LogError("Message: " + ex2.Message);
            return false;
          }
        }
      }
      // Aqui termina a transação
    }
    catch (Exception e)
    {
      _logger.LogError("Falha ao desfragmentar a tabela: " + tableName);
      _logger.LogError(e.ToString());
      return false;
    }
  }

  private bool checkClusteredIndex(string tableName)
  {
    logDebug("Verificando se a tabela " + tableName + " pussui indice cluster");

    StringBuilder sql = new StringBuilder();

    sql.Append("SELECT count(*) as 'count' FROM sys.indexes \n");
    sql.Append("WHERE object_id = OBJECT_ID('" + tableName + "') AND index_id = 1");

    var ds = ExecSql(sql.ToString());

    int count = int.Parse(ds.Tables[0].Rows[0]["count"].ToString());

    // Confere se a tabela pussui index cluster
    // count > 0, há index cluster
    // count == 0, não há index cluster

    return count > 0;
  }

  private void VerifyLock(string tableName)
  {
    logDebug("Verificando se a tabela " + tableName + " está bloqueada");

    StringBuilder sql = new StringBuilder();

    sql.Append("SELECT count(*) as 'count' from sys.dm_tran_locks\n");
    sql.Append("where resource_associated_entity_id = object_id('" + tableName + "')");

    var ds = ExecSql(sql.ToString());

    int count = int.Parse(ds.Tables[0].Rows[0]["count"].ToString());

    // Confere se a tabela esta bloqueada
    // count > 0, há locks
    // count == 0, não há locks

    if (count > 0)
    {
      throw new Exception("Current table is locked");
    }
  }

  private DataSet ExecSql(string SqlCommand)
  {
    using (var conn = new SqlConnection(_connString))
    {
      var ds = new DataSet();

      logDebug("Abrindo conexão");
      conn.Open();

      logDebug("Executando Sql: " + SqlCommand);
      var command = new SqlCommand(SqlCommand, conn);

      var adapter = new SqlDataAdapter(command);

      adapter.Fill(ds);

      return ds;
    }
  }

  private void logDebug(string msg)
  {
    if (_debug == 1) _logger.LogTrace(msg);
  }
}
