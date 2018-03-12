cmake_minimum_required(VERSION 3.8)
project(pdm)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        proj1/src/rbf/fm_util.h
        proj1/src/rbf/pageHeader.h
        proj1/src/rbf/pfm.cc
        proj1/src/rbf/pfm.h
        proj1/src/rbf/rbfm.cc
        proj1/src/rbf/rbfm.h
        proj1/src/rbf/rbfm_util.h
        proj1/src/rbf/test_util.h
        proj1/src/rm/rm.cc
        proj1/src/rm/rm.h
        proj1/src/rm/rm_test_util.h
#        proj1/src/rbf/rbftest1.cc
#        proj1/src/rbf/rbftest10.cc
#        proj1/src/rbf/rbftest11.cc
#        proj1/src/rbf/rbftest12.cc
#        proj1/src/rbf/rbftest2.cc
#        proj1/src/rbf/rbftest3.cc
#        proj1/src/rbf/rbftest4.cc
#        proj1/src/rbf/rbftest5.cc
#        proj1/src/rbf/rbftest6.cc
#        proj1/src/rbf/rbftest7.cc
#        proj1/src/rbf/rbftest8.cc
#        proj1/src/rbf/rbftest8b.cc
#        proj1/src/rbf/rbftest9.cc
#        proj1/src/rbf/rbftest_delete.cc
#        proj1/src/rbf/rbftest_p0.cc
#        proj1/src/rbf/rbftest_p1.cc
#        proj1/src/rbf/rbftest_p1b.cc
#        proj1/src/rbf/rbftest_p1c.cc
#        proj1/src/rbf/rbftest_p2.cc
#        proj1/src/rbf/rbftest_p2b.cc
#        proj1/src/rbf/rbftest_p3.cc
#        proj1/src/rbf/rbftest_p4.cc
#        proj1/src/rbf/rbftest_p5.cc
#        proj1/src/rbf/rbftest_update.cc
#        proj1/src/rm/rmtest_00.cc
#        proj1/src/rm/rmtest_01.cc
#        proj1/src/rm/rmtest_02.cc
#        proj1/src/rm/rmtest_03.cc
#        proj1/src/rm/rmtest_04.cc
#        proj1/src/rm/rmtest_05.cc
#        proj1/src/rm/rmtest_06.cc
#        proj1/src/rm/rmtest_07.cc
#        proj1/src/rm/rmtest_08.cc
#        proj1/src/rm/rmtest_09.cc
#        proj1/src/rm/rmtest_10.cc
#        proj1/src/rm/rmtest_11.cc
#        proj1/src/rm/rmtest_12.cc
#        proj1/src/rm/rmtest_13.cc
#        proj1/src/rm/rmtest_13b.cc
#        proj1/src/rm/rmtest_14.cc
#        proj1/src/rm/rmtest_15.cc
#        proj1/src/rm/rmtest_create_tables.cc
#        proj1/src/rm/rmtest_delete_tables.cc
#        proj1/src/rm/rmtest_extra_1.cc
#        proj1/src/rm/rmtest_extra_2.cc
        proj1/src/ix/ix.cc
        proj1/src/ix/ix.h

        proj1/src/qe/qe.cc
        proj1/src/qe/qe.h
        proj1/test/qe/qetest_01.cc
        proj1/src/qe/qe_test_util.h
        )

add_executable(pdm ${SOURCE_FILES})