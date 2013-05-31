import testutils

# ---------------------------------------------------------------------------------
def test_qsub_no_options_passed():
    """
    qsub test run: no_options_passed

        Command Output:
          No required options entered
          'time' not provided
          

    """

    args      = """/bin/ls"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_non_existant_option():
    """
    qsub test run: non_existant_option

        Command Output:
          Usage: qsub.py [options] <executable> [<excutable options>]
          
          qsub.py: error: no such option: -z
          

    """

    args      = """-z -t10 -n10 /bin/ls"""
    exp_rs    = 512

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_debug_flag_only_1():
    """
    qsub test run: debug_flag_only_1

        Command Output:
          
          qsub.py -d
          
          No required options entered
          'time' not provided
          

    """

    args      = """-d"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_debug_flag_only_2():
    """
    qsub test run: debug_flag_only_2

        Command Output:
          
          qsub.py -debug
          
          'time' not provided
          

    """

    args      = """-debug"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_verbose_flag_only():
    """
    qsub test run: verbose_flag_only

        Command Output:
          No required options entered
          'time' not provided
          

    """

    args      = """-v"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_non_integer_nodecount():
    """
    qsub test run: non_integer_nodecount

        Command Output:
          Usage: qsub.py [options] <executable> [<excutable options>]
          
          qsub.py: error: option -n: invalid integer value: 'five'
          

    """

    args      = """--mode smp -t50 -nfive --geometry 40x40x50x50   /bin/ls"""
    exp_rs    = 512

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_non_realistic_nodecount():
    """
    qsub test run: non_realistic_nodecount

        Command Output:
          node count out of realistic range
          

    """

    args      = """--mode smp -t50 -n2048 --geometry 40x40x50x50x1 /bin/ls"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_invalid_geometry_1():
    """
    qsub test run: invalid_geometry_1

        Command Output:
          Invalid geometry entered: 
          

    """

    args      = """--mode smp -t50 -n10 --geometry x /bin/ls"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_no_roject_specified():
    """
    qsub test run: no_roject_specified

        Command Output:
          'time' not provided
          

    """

    args      = """-A -t50 -n10 /bin/ls"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_project_specified():
    """
    qsub test run: project_specified

        Command Output:
          2
          

    """

    args      = """-A who -t50 -n10 /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_Check_attrs_1():
    """
    qsub test run: Check_attrs_1

        Command Output:
          3
          

    """

    args      = """--attrs xxxx -t50 -n10 /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_Check_attrs_2():
    """
    qsub test run: Check_attrs_2

        Command Output:
          4
          

    """

    args      = """--attrs 1111 -t50 -n10 /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_Check_attrs_3():
    """
    qsub test run: Check_attrs_3

        Command Output:
          5
          

    """

    args      = """--attrs xx=:yy -t50 -n10 /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_Check_attrs_4():
    """
    qsub test run: Check_attrs_4

        Command Output:
          6
          

    """

    args      = """--attrs xx=one:yy=1:zz=1one -t50 -n10 /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_cwd_option_1():
    """
    qsub test run: cwd_option_1

        Command Output:
          7
          

    """

    args      = """--cwd /tmp/ -t10 -n 10 -e p /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_cwd_option_2():
    """
    qsub test run: cwd_option_2

        Command Output:
          8
          

    """

    args      = """--cwd /tmp -t10 -n 10 -e p /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_cwd_option_3():
    """
    qsub test run: cwd_option_3

        Command Output:
          directory /x/p does not exist
          

    """

    args      = """--cwd /x -t10 -n 10 -e p /bin/ls"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_cwd_option_4():
    """
    qsub test run: cwd_option_4

        Command Output:
          9
          

    """

    args      = """--cwd /tmp/ -t10 -n 10 -e p -o x /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_cwd_option_5():
    """
    qsub test run: cwd_option_5

        Command Output:
          10
          

    """

    args      = """--cwd /tmp -t10 -n 10 -e p -o x /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_debuglog_option():
    """
    qsub test run: debuglog_option

        Command Output:
          11
          

    """

    args      = """-t10 -n 10 -e p -o x --debuglog y /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_inputfile_option_1():
    """
    qsub test run: inputfile_option_1

        Command Output:
          file /Users/georgerojas/p/Cobalt/cobalt/testsuite/TestCobaltClients/none not found, or is not a file
          

    """

    args      = """-i none -t10 -n 10 /bin/ls"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_inputfile_option_2():
    """
    qsub test run: inputfile_option_2

        Command Output:
          12
          

    """

    args      = """-i y -t10 -n 10 /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_email_option():
    """
    qsub test run: email_option

        Command Output:
          13
          

    """

    args      = """-M g -t10 -n10 /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_outputprefix():
    """
    qsub test run: outputprefix

        Command Output:
          14
          WARNING: failed to create cobalt log file at: /tmp.cobaltlog
                   Permission denied
          

    """

    args      = """-O /tmp -t10 -n10 /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_invalid_user():
    """
    qsub test run: invalid_user

        Command Output:
          Usage: qsub.py [options] <executable> [<excutable options>]
          
          qsub.py: error: no such option: -r
          

    """

    args      = """-run_users naughtyuser -t10 -n10 /bin/ls"""
    exp_rs    = 512

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_mode_option_2():
    """
    qsub test run: mode_option_2

        Command Output:
          15
          

    """

    args      = """-t10 -n512 --proccount 1023 --mode vn /bin/ls"""
    exp_rs    = 0

    results = testutils.run_cmd('qsub.py',args,None) 
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
def test_qsub_mode_option_4():
    """
    qsub test run: mode_option_4

        Command Output:
          node count out of realistic range
          

    """

    args      = """-A Acceptance -q testing -n 49152 -t 60 --mode script /bin/ls"""
    exp_rs    = 256

    results = testutils.run_cmd('qsub.py',args,None) 
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