import testutils

# ---------------------------------------------------------------------------------
def test_showres_arg_1():
    """
    showres test run: arg_1

        Command Output:
          Reservation  Queue  User  Start                                 Duration  Passthrough  Partitions        
          =========================================================================================================
          george       q_1    None  Wed May 31 20:00:00 2023 +0000 (UTC)  05:00     Allowed      ANL-R00-R01-2048  
          

    """

    args      = ''
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_arg_2():
    """
    showres test run: arg_2

        Command Output:
          Reservation  Queue  User  Start                     Duration  Passthrough  Partitions        
          =============================================================================================
          george       q_1    None  Wed May 31 15:00:00 2023  05:00     Allowed      ANL-R00-R01-2048  
          

    """

    args      = """--oldts"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_arg_3():
    """
    showres test run: arg_3

        Command Output:
          No arguments needed
          Reservation  Queue  User  Start                                 Duration  Passthrough  Partitions        
          =========================================================================================================
          george       q_1    None  Wed May 31 20:00:00 2023 +0000 (UTC)  05:00     Allowed      ANL-R00-R01-2048  
          

    """

    args      = """arg1"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_l_option_1():
    """
    showres test run: l_option_1

        Command Output:
          Reservation  Queue  User  Start                                 Duration  End Time                              Cycle Time  Passthrough  Partitions        
          ===========================================================================================================================================================
          george       q_1    None  Wed May 31 20:00:00 2023 +0000 (UTC)  05:00     Thu Jun  1 01:00:00 2023 +0000 (UTC)  None        Allowed      ANL-R00-R01-2048  
          

    """

    args      = """-l"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_l_option_2():
    """
    showres test run: l_option_2

        Command Output:
          Reservation  Queue  User  Start                     Duration  End Time                  Cycle Time  Passthrough  Partitions        
          ===================================================================================================================================
          george       q_1    None  Wed May 31 15:00:00 2023  05:00     Wed May 31 20:00:00 2023  None        Allowed      ANL-R00-R01-2048  
          

    """

    args      = """-l --oldts"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_x_option_1():
    """
    showres test run: x_option_1

        Command Output:
          Reservation  Queue  User  Start                                 Duration  End Time                              Cycle Time  Passthrough  Partitions        Project  ResID  CycleID  
          ====================================================================================================================================================================================
          george       q_1    None  Wed May 31 20:00:00 2023 +0000 (UTC)  05:00     Thu Jun  1 01:00:00 2023 +0000 (UTC)  None        Allowed      ANL-R00-R01-2048  None     1      -        
          

    """

    args      = """-x"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_x_option_1():
    """
    showres test run: x_option_1

        Command Output:
          Reservation  Queue  User  Start                     Duration  End Time                  Cycle Time  Passthrough  Partitions        Project  ResID  CycleID  
          ============================================================================================================================================================
          george       q_1    None  Wed May 31 15:00:00 2023  05:00     Wed May 31 20:00:00 2023  None        Allowed      ANL-R00-R01-2048  None     1      -        
          

    """

    args      = """-x --oldts"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_combo():
    """
    showres test run: combo

        Command Output:
          Only use -l or -x not both
          

    """

    args      = """-l -x"""
    exp_rs    = 256

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_help_1():
    """
    showres test run: help_1

        Command Output:
          Usage: showres [-l] [-x] [--oldts] [--version]
          
          Options:
            --version    show program's version number and exit
            -h, --help   show this help message and exit
            -d, --debug  turn on communication debugging
            -l           print reservation list verbose
            --oldts      use old timestamp
            -x           print reservations really verbose
          

    """

    args      = """--help"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_help_2():
    """
    showres test run: help_2

        Command Output:
          Usage: showres [-l] [-x] [--oldts] [--version]
          
          Options:
            --version    show program's version number and exit
            -h, --help   show this help message and exit
            -d, --debug  turn on communication debugging
            -l           print reservation list verbose
            --oldts      use old timestamp
            -x           print reservations really verbose
          

    """

    args      = """-h"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_version():
    """
    showres test run: version

        Command Output:
          version: "showres.py " + $Revision: 2154 $ + , Cobalt  + $Version$
          

    """

    args      = """--version"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg

# ---------------------------------------------------------------------------------
def test_showres_debug():
    """
    showres test run: debug

        Command Output:
          
          showres.py --debug
          
          component: "system.get_implementation", defer: False
            get_implementation(
               )
          
          
          component: "scheduler.get_reservations", defer: False
            get_reservations(
               [{'users': '*', 'block_passthrough': '*', 'duration': '*', 'cycle': '*', 'project': '*', 'cycle_id': '*', 'name': '*', 'queue': '*', 'start': '*', 'partitions': '*', 'res_id': '*'}],
               )
          
          
          Reservation  Queue  User  Start                                 Duration  Passthrough  Partitions        
          =========================================================================================================
          george       q_1    None  Wed May 31 20:00:00 2023 +0000 (UTC)  05:00     Allowed      ANL-R00-R01-2048  
          

    """

    args      = """--debug"""
    exp_rs    = 0

    results = testutils.run_cmd('showres.py',args,None) 
    rs      = results[0]
    cmd_out = results[1]

    # Test Pass Criterias
    no_rs_err     = (rs == exp_rs)
    no_fatal_exc  = (cmd_out.find("FATAL EXCEPTION") == -1)

    result = no_rs_err and no_fatal_exc

    errmsg  = "\n\nFailed Data:\n\n" \
        "Return Status %s, Expected Return Status %s\n\n" \
        "Command Output:\n%s\n\n" \
        "Arguments: %s" % (str(rs), str(exp_rs), str(cmd_out), args)

    assert result, errmsg