#!/usr/bin/perl

use strict;
use warnings;
#no warnings 'uninitialized';
use DBI;

my $DB=$ARGV[0];
my $port=$ARGV[1];
my $username=$ARGV[2];
my $pass=$ARGV[3];
my $query=$ARGV[4];

my $connstr = "ENG=$DB;CommLinks=tcpip{host=localhost;port=$port};UID=$username;PWD=$pass";
my $dbh = DBI->connect( "DBI:SQLAnywhere:$connstr", '', '', {AutoCommit => 1} ) or warn $DBI::errstr;
my $sel_stmt=$dbh->prepare($query) or warn $DBI::errstr;
$sel_stmt->execute() or warn $DBI::errstr;
my $result = $sel_stmt->fetchall_arrayref();
$sel_stmt->finish();

my $file = 'SqlOutput.txt';
open FILE, '>', $file or die "Could not open '$file', No such file found in the provided path $!\n";

foreach my $row (@$result)
{
my $len = @$row;
if($len eq 1)
{
print FILE @$row;
}
else{
my $data=join(" @@ ",@$row);
print FILE $data;}
print FILE "\n";
}

close(FILE);
$dbh->disconnect;