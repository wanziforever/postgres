#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for tcop
#
# IDENTIFICATION
#    src/backend/tcop/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/tcop/hgdispatch
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = \
	hgdispatch_connection.o \
	hgdispatch_utility.o \
	hgdispatch.o \
	pqwrapper.o \
	extendhgdispatch.o \
	hgdirtyoids.o


include $(top_srcdir)/src/backend/common.mk
