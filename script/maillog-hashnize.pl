#!/usr/bin/perl
# "pflog" enhancement for parsing maillog of postfix 2.3 or higher version.
# "pflog" see: http://www.tmtm.org/ruby/pflog/pflog-0.3

use strict;
use warnings;

use Time::Local;
use Getopt::Long;
#use Data::Dumper;

my %month = (qw(Jan 1 Feb 2 Mar 3 Apr 4 May 5 Jun 6 Jul 7 Aug 8 Sep 9 Oct 10 Nov 11 Dec 12));

my $SEPARATOR = q(,);

my (%queue, @found_queue);

GetOptions(
    'year|y=s'    => \ my $year,
    'no-header|H' => \ my $no_header,
    'help'        => \ my $help,
);

if ( $help ) {
    print <<END_HELP;
Usage:
  $0 -y YEAR mail.log > mail.csv
END_HELP
    exit;
}

if ( !defined $year || $year !~ /^\d{4}$/ ) {
    die "specify -y YEAR (YEAR is 4 letter digits, e.g. 2011).\n";
}

my $re_date = qr/[A-Z][a-z][a-z]  ?\d+ \d{2}:\d{2}:\d{2}/;
my $re_host = qr/\S+/;
#my $re_following_capture = qr/\s*\(([^()]+)\)/;

my $re_line = qr{^($re_date) ($re_host) postfix/(\w+)\[\d+\]: (\w+):\s*(.*)};

while (<>) {
    my ($date, $host, $service, $queue_id, $following) = /$re_line/
        or next;
    if ( !exists $queue{$queue_id} ) {
        $queue{$queue_id} = {};
        # queue_id は見つかった順番に記録される
        # queue_id is recorded order by found.
        push @found_queue, $queue_id;
    }
    my $q = $queue{$queue_id};
    my ($information) = $following =~ /\s*\((.+)\)$/
        and $following =~ s/\s\(.+\)$//;
    my %param;
    if ( $following =~ /=/ ) {
        my @param = map { split /=/, $_, 2 } split /,\s*/, $following;
        if ( @param % 2 == 0 ) {
            %param = @param;
        }
        else {
            warn "found odd number of key/value pair.";
        }
    }

    # %param 手直し
    # %param modification.
    if ( exists $param{client} && defined $param{client} ) {
        my ($hostname, $ipaddr) = $param{client} =~ /^(.+?)\[([0-9.]+)\]/;
        $param{client_hostname} = $hostname;
        $param{client_ipaddr}   = $ipaddr;
    }
    if ( $information && $param{status} ) {
        $param{information} = $information;
    }
    for my $key ( qw(from to) ) {
        if ( defined $param{$key} && $param{$key} =~ /^<(.*)>$/ ) {
            $param{$key} = $1;
        }
    }

    # 今回の行で取得することができた情報を追加
    # Addition of information that be got current line.
    for my $key (keys %param) {
        my $value = $param{$key};
        if ( !exists $q->{$key} ) {
            # 新規採用 / newly
            $q->{$key} = $value;
        }
        elsif ( !ref $q->{$key} ) { # 文字列 / string
            # 配列リファレンスにして追加 / Addition as array reference
            $q->{$key} = [$q->{$key}, $value];
        }
        elsif ( ref $q->{$key} eq 'ARRAY' ) {
            # 配列リファレンスに push / push to array reference
            push @{$q->{$key}}, $value;
        }
        else {
            die "unknown situation."; # 想定外 / non-supposition
        }
    }

    # 12/31 -> 01/01 などの流れの場合の年の調整
    # Adjustoment flow of year on 12/31 -> 01/01
    skew_date($date)
        and $year++;

    # Add _meta
    my $meta_q = $q->{_meta} ||= {};
    if ( !defined $meta_q->{host} ) {
        $meta_q->{host} = $host;
    }
    if ( $param{client} || $param{uid} ) {
        $meta_q->{start_date} = date_format($date);
    }
    if ( $param{status} && $param{status} eq 'sent' ) {
        $meta_q->{success}++;
        $meta_q->{end_date} = date_format($date);
    }
}

#print Dumper(\%queue);

# ### DEBUG:
# my @list = map { [$_ => $queue{$_}] } @found_queue;
# print Dumper(@list);
# exit;

# ヘッダ出力
# Header output
if ( !$no_header && @found_queue ) {
    # 内容行 (@found_queue) が見つからなかったら
    # ヘッダ行も出力しないとした
    # 内容がなければファイルサイズが 0 のほうが
    # パッと見てわかりやすいからという意図
    # If content row is not found, we do not output header line too.
    # It is cleary that 
    printf "%s\n", join $SEPARATOR, map { s/^\s+//; qq("$_") } split /\n/, <<END_LIST;
  queue id
  arrived time
  processed time
  smtp client hostname / uid
  smtp client IP address / username
  envelope from
  envelope to
  message-id
  status
  relay to
  delay time
  size
  information (reason of defered, local mailbox name, successful message...)
END_LIST
}

### pflog compatible output
for my $queue_id (@found_queue) {
    my $q = $queue{$queue_id}; # 'HASH'

    my @row = ($queue_id,
               @{$q->{_meta}}{qw(start_date end_date)},
               @$q{qw(client_hostname client_ipaddr from to message-id status relay delay size information)});

    for ( grep { ref $_ eq 'ARRAY' } @row ) {
        $_ = join $SEPARATOR, @$_;
    }

    for (@row) {
        $_ = '' if !defined $_;
        if ( /,/ || /"/ ) {
            s/"/""/g;
            $_ = qq("$_");
        }
        elsif ( !/^\d+(?:\.\d+)?$/ && length $_ ) {
            # Excel は数字のみではないものならダブルクォートするので
            # それを真似る
            # Excel quotes not only digits,
            # so this program imimtations it.
            $_ = qq("$_");
        }
        elsif ( /^\d{11}$/ ) {
            # queue id (と思われるもの)がたまたま全部数字だった場合
            # queue id (we think so) character is all digits unexpectedly.
            $_ = qq("$_");
        }
    }
    printf "%s\n", join $SEPARATOR, @row;
}

### from pflog
# output:
#  queue id
#  arrived time
#  processed time
#  smtp client hostname / uid
#  smtp client IP address / username
#  envelope from
#  envelope to
#  message-id
#  status
#  relay to
#  delay time
#  size
#  information (reason of defered, local mailbox name, successful message...)

sub date_format {
    my $date_str = shift; # e.g. Jan 31 00:00:01
    my ($mon_name, $day, $hhmmss) = split /\s+/, $date_str;
#    my ($hh, $mm, $ss) = map { sprintf '%d', $_ } split /:/, $hhmmss;
    # sprintf に8進数と勘違いされないように
    # We avoid that sprintf confuses it octet.
    $day =~ s/^0//;
    my $mon = $month{$mon_name};
    return sprintf '%d/%02d/%02d %s', $year, $mon, $day, $hhmmss;
}

# skew_date($syslog_date_string)
# 前回 skew_date を呼び出したときの日付(年月日)よりも逆行しているなら真
# 分や秒の細かいことまで見ない
# Ture if Date (year month day) previous calling of skwe_date goes backward.
# We do not see detail of minutes or second.
{
my $prev_mm_dd; # state
sub skew_date {
    my $cur_mm_dd = join '/', (split m{/}, date_format(shift))[1,2];

    my $is_skew;
    if (    !$prev_mm_dd              # 初回呼び出し / initial calling
         || $prev_mm_dd le $cur_mm_dd # 順番通り / right order (e.g. 07/22 le 07/23)
     ) {
        $is_skew = 0;
    }
    else {
        $is_skew = 1;
    }
    $prev_mm_dd = $cur_mm_dd;
    return $is_skew;
}
}

__END__

=pod

=encoding utf-8

=head1 NAME

maillog-hashnize.pl - enhancement of "pflog" for parsing maillog of postfix 2.3 or higher version.

=head1 SYNOPSIS

 # input postfix log of syslog format, output csv format.
 maillog-hasnize.pl -y 2011 mail.log > maillog.csv

=head1 DESCRIPTIONS

this program is postfix "mail.log" parser, convert from syslog format to csv format for MS-Excel and more spreadsheet viewer.

=head1 LIMITATION

because syslog format does not include "year" information,
specify the "mail.log"'s year as "-y" option.

support from the year to from *next* new year.
but years are missing from the "mail.log", e.g. next "2009/??/??" line is "2011/??/??", it is not support that leap 2 year and more.

=head1 ACKNOWLEDGEMENT

pflog: L<http://www.tmtm.org/ruby/pflog/pflog-0.3>

=head1 COPYRIGHT AND LICENCE

Copyright 2010-2011 fonfun corporation.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut
