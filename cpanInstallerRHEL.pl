#!/usr/bin/perl
# ****************************************************************************************
# Ericsson Radio Systems AB							SCRIPT
# ****************************************************************************************
#
# (c) Ericsson Radio Systems AB 2019 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Radio Systems AB, Sweden. The programs may be used
# and/or copied only with the written permission from Ericsson Radio
# Systems AB or in accordance with the terms and conditions stipulated
# in the agreement/contract under which the program(s) have been
# supplied.
#
#
# ****************************************************************************************
# Name    : cpanInstallerRHEL.pl
# Purpose : Installation of CPAN modules in RHEL for RT execution
# Usage   : perl cpanInstallerRHEL.pl <CPAN_PACKAGE>
# Author  : XARJSIN
# ****************************************************************************************
#
#-- get current directory
use Cwd;
use Cwd 'abs_path','realpath','fast_abs_path';

if ( $< != 0 ) {
	print "\n\tThis script must be executed as root user\n\n";
        exit (0);
}

if (@ARGV < 1) {
	print STDERR "\n\tUsage: perl $0 <CPAN_Modules.zip>\n\n";
	exit 1;
}

print "\n\tCPAN installer for linux triggered\n\n";

########################################################
# Create REPO for prerequisite installation
system("perl /eniq/home/dcuser/BT-FT_Script/RhelRepoScript.pl create");

my $cpanPkg = $ARGV[0];

$ENV{'PATH'} = '/usr/sbin:/usr/bin:/usr/sfw/bin';
print $ENV{'PATH'};

my $cpanDir = "CPAN_Modules";

if ( -d "$cpanDir" ) {
	system("/usr/bin/rm -rf $cpanDir");
}

system("/usr/bin/unzip $cpanPkg");
sleep(3);

chdir("$cpanDir") or die "cannot change: $!\n";

my @modules = ();
$modules[0]="XML-Writer";
$modules[1]="XML-NamespaceSupport";
$modules[2]="Devel-Symdump";
$modules[3]="Pod-Parser";
$modules[4]="Pod-Coverage";
$modules[5]="Test-Pod-Coverage";
$modules[6]="Pod-Escapes";
$modules[7]="Test";
$modules[8]="Pod-Simple";
$modules[9]="Sub-Uplevel";
$modules[10]="Test-Simple";
$modules[11]="Test-Pod";
$modules[12]="Abstract-Meta-Class";
$modules[13]="DBIx-Connection";
$modules[14]="XML-SAX-Base";
$modules[15]="XML-SAX";
$modules[16]="Simple-SAX-Serializer";
$modules[17]="Test-DBUnit";
$modules[18]="Text-CSV";
$modules[19]="File-Slurp";
$modules[20]="DBD-SQLAnywhere";
$modules[21]="Test-Exception";
$modules[22]="IO-Tty";
$modules[23]="Expect";
$modules[24]="MCE";
$modules[25]="XML-Simple";

system("/usr/bin/yum install -y glibc-devel");
system("/usr/bin/yum install -y gcc");
system("/usr/bin/yum install -y perl-core");
system("/usr/bin/yum install -y perl-devel");
system("/usr/bin/yum install -y perl-Test-Harness");
system("/usr/bin/yum install -y perl-DBI");

foreach my $module (@modules)
{
	my $tar_fname;
	my @FILE_LIST = glob "$module*gz";
	if (($#FILE_LIST + 1) != 0)
	{
		my $gz_fname = $FILE_LIST[$#FILE_LIST];
		system("gunzip $gz_fname");
		$tar_fname = substr($gz_fname,0,index($gz_fname,".gz"));
	}
	else
	{
		my @FILE_LIST = glob "$module*tar";
		$tar_fname = $FILE_LIST[$#FILE_LIST];
	}	
	system("tar xvf $tar_fname");
	my $dir_name = substr($tar_fname,0,index($tar_fname,".tar"));
	print "Module Name : $dir_name\n";

	if ( -d "$dir_name" ) {
		system("chmod -R 750 $dir_name");
		chdir($dir_name);
		system("/usr/bin/perl Makefile.PL");
		sleep(3);
		system("/usr/bin/make");
		sleep(2);
		system("/usr/bin/make test");
		system("/usr/bin/make install");
		chdir("..");
	}
}
system("python3 -m pip install /eniq/home/dcuser/BT-FT_Script/openpyxl-3.1.2.tar.gz");

chdir("..");

########################################################
# Clean up REPO after prerequisite installation
system("perl /eniq/home/dcuser/BT-FT_Script/RhelRepoScript.pl clean");
