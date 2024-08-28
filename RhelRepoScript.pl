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
# Name    : RhelRepoScript.pl
# Purpose : Creation and Deletion of RHEL repos for RT execution
# Usage   : perl RhelRepoScript.pl create|clean
# Author  : XARJSIN
# ****************************************************************************************
use warnings;
use strict;

if (@ARGV != 1) {
	print STDERR "\n\tUsage: perl $0 create|clean\n\n";
	exit 1;
}
my $FIND = "/usr/bin/find";
my $repoFile = "/etc/yum.repos.d/local.repo";

sub getMwsUpgradeLatest{
	my $oldRel1 = 0;
	my $oldRel2 = 0;
	my $oldRel3 = 0;
	my $mwsip = "";
	my $latestPatch = "";
	my $patchpath = "";
	open(MWSPROPS, '< /eniq/home/dcuser/BT-FT_Script/mws.properties') or warn("Cannot read mws.properties file!!\n");
	my @mwsFile = <MWSPROPS>;
	chomp(@mwsFile);
	close MWSPROPS;
	foreach my $path (@mwsFile){
		$_ = $path;
		if(/^MWS IP=/){
			my @input = split("=",$path);
			$mwsip = $input[1];
		}
		if(/^PATCH=/){
			my @input = split("=",$path);
			$patchpath = $input[1];
		}
	}
	$latestPatch = '/net/'.$mwsip.'/'.$patchpath.'/';
	return $latestPatch;
}

if ($ARGV[0] eq "create"){
	my $mwsBasePath = getMwsUpgradeLatest();
	open(REPOFILE, "> $repoFile") or warn("Cannot open repo file!!\n");
	print REPOFILE "[LocalRepo]\n";
	print REPOFILE "name=LocalRepository\n";
	print REPOFILE "baseurl=file://$mwsBasePath\n";
	print REPOFILE "enabled=1\n";
	print REPOFILE "gpgcheck=0\n";
	close(REPOFILE);
	system("/usr/bin/yum clean all");
	system("/usr/bin/yum repolist");
	print "\nREPO CREATION COMPLETE!\n";
}
elsif ($ARGV[0] eq "clean"){
	unlink $repoFile;
	system("/usr/bin/yum clean all");
	system("/usr/bin/yum repolist");
	print "\nREPO CLEANUP COMPLETE!\n";
}
else{
	print STDERR "\nINCORRECT ARGUMENT!\n";
}