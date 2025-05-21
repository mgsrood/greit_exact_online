import pytest
from ...utils.config import ConfigManager 
import sql_script.utils.log as log_module

def test_config_manager_initialization():
    """Test de basis initialisatie van ConfigManager."""
    conn_str = "dummy_connection_string"
    auth_method = "SQL"
    tenant_id = "dummy_tenant"
    client_id = "dummy_client"
    client_secret = "dummy_secret"
    script_name = "TestScript"

    manager = ConfigManager(
        connection_string=conn_str,
        auth_method=auth_method,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        script_name=script_name
    )

    assert manager.connection_string == conn_str
    assert manager.auth_method == auth_method
    assert manager.tenant_id == tenant_id
    assert manager.client_id == client_id
    assert manager.client_secret == client_secret
    assert manager.script_name == script_name
    assert manager.logger is None
    assert manager.klant is None
    assert manager.script_id is None

def test_config_manager_update_klant():
    """Test de update_klant methode."""
    # Maak een ConfigManager instantie met minimale argumenten
    # Connection_string is vereist door __init__
    manager = ConfigManager(connection_string="dummy_conn_str_for_update_klant")
    
    nieuwe_klant = "TestKlant"
    manager.update_klant(nieuwe_klant)
    assert manager.klant == nieuwe_klant

def test_setup_logger_success(mocker):
    """Test succesvolle ConfigManager.setup_logger."""
    mock_db_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_cursor.fetchone.return_value = (100,) 
    
    mocker.patch.object(ConfigManager, 'get_connection', return_value=mock_db_conn)
    mock_db_conn.__enter__.return_value = mock_db_conn 
    mock_db_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_actual_setup_logging = mocker.patch.object(log_module, 'setup_logging')
    mock_logger_object = mocker.MagicMock()
    mock_actual_setup_logging.return_value = mock_logger_object

    test_conn_str = "test_conn_string_for_logging"
    test_script_name = "DataImportTest"
    test_klant_naam = "KlantX"
    
    # Geef alle verwachte __init__ argumenten mee, ook al zijn ze None voor deze test.
    manager = ConfigManager(
        connection_string=test_conn_str, 
        auth_method="SQL",  # Standaard, maar expliciet voor duidelijkheid
        tenant_id=None,
        client_id=None,
        client_secret=None,
        script_name=test_script_name
    )
    
    script_id = manager.setup_logger(klant=test_klant_naam)

    assert script_id == 101
    assert manager.script_id == 101
    assert manager.klant == test_klant_naam
    assert manager.logger == mock_logger_object

    mock_cursor.execute.assert_called_once_with('SELECT MAX(ScriptID) FROM Logging')
    
    mock_actual_setup_logging.assert_called_once_with(
        conn_str=test_conn_str,
        klant=test_klant_naam,
        script=test_script_name,
        script_id=101,
        auth_method=manager.auth_method,
        tenant_id=manager.tenant_id,
        client_id=manager.client_id,
        client_secret=manager.client_secret
    )

def test_setup_logger_db_error_getting_script_id(mocker):
    """Test ConfigManager.setup_logger wanneer de database een error geeft bij het ophalen van MAX(ScriptID)."""
    mocker.patch.object(ConfigManager, 'get_connection', side_effect=Exception("Database connection error"))
    mock_logging_error = mocker.patch('sql_script.utils.config.logging.error')

    # Geef alle verwachte __init__ argumenten mee
    manager = ConfigManager(
        connection_string="dummy_conn_str_for_error_test",
        auth_method="SQL",
        tenant_id=None,
        client_id=None,
        client_secret=None,
        script_name="ErrorTestScript"
    )
    script_id = manager.setup_logger(klant="TestKlantError")

    assert script_id is None
    assert manager.logger is None
    mock_logging_error.assert_called_once() 