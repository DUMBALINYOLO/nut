#!/usr/bin/env python
import argparse
import logging
import select
import socket
import re
from threading import Event, Thread


_global_kill = Event()


class UdpRelay(Thread):

    def __init__(
            self,
            host_a, host_b, port,
            max_message_size=16384,
            timeout=1.0):
        super(UdpRelay, self).__init__()

        self.host_a = host_a
        self.host_b = host_b
        self.port = port
        self.max_message_size = max_message_size
        self.timeout = timeout

        self._logger = logging.getLogger(
            'relay.UdpRelay({host_a} <-> {host_b}, {port})'
            .format(**self.__dict__))

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.socket.settimeout(self.timeout)
        self._logger.info(
            'Successfully set socket timeout to {timeout}'
            .format(**self.__dict__))

        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._logger.info('Successfully set socket option to SO_REUSEADDR')

        self.socket.bind(('', self.port))
        self._logger.info(
            'Successfully bound socket to ("", {port})'
            .format(**self.__dict__))

    def __del__(self):
        self._logger.info('Closing socket')
        self.socket.close()

    def run(self):
        global _global_kill

        self._logger.info('Starting listening loop...')
        while True:
            try:
                if _global_kill.isSet():
                    self._logger.info('Received global kill')
                    break

                msg, (src_host, src_port) = \
                    self.socket.recvfrom(self.max_message_size)
                self._handleMessage(msg, src_host, src_port)

            except socket.timeout:
                pass

            except Exception as e:
                self._logger.error(
                    'Caught exception while waiting for "recvfrom": {:}'
                    .format(e))
                break

        self._logger.info('Terminating')

    def _handleMessage(self, msg, src_host, src_port):
        self._logger.debug('Message received: {!r}'.format(msg))

        if src_host == self.host_a:
            src_name, dst_name = 'A', 'B'
            dst_host = self.host_b
        elif src_host == self.host_b:
            src_name, dst_name = 'B', 'A'
            dst_host = self.host_a
        else:
            self._logger.debug(
                'Message received from unknown host ({!s}), ignoring'
                .format(src_host))
            return

        self._routeMessage( msg, src_name, src_host, src_port,
                                 dst_name, dst_host, self.port )
                            
    def _routeMessage( self, msg, src_name, src_host, src_port,
                                  dst_name, dst_host, dst_port ):
        self._logger.debug(
            'Message received from host {:} ({!s}:{!s}), routing to host {:} '
            '({!s}:{!s})'
            .format(
                src_name, src_host, src_port,
                dst_name, dst_host, dst_port))

        self.socket.sendto(msg, (dst_host, dst_port))


class TcpRelay(Thread):

    def __init__(
            self,
            socket_a, addr_a, socket_b, addr_b,
            max_message_size=16384,
            timeout=1.0):
        super(TcpRelay, self).__init__()

        self.socket_a = socket_a
        self.host_a, self.port_a = addr_a
        self.socket_b = socket_b
        self.host_b, self.port_b = addr_b
        self.max_message_size = max_message_size
        self.timeout = timeout

        self._sockets = [self.socket_a, self.socket_b]

        self._logger = logging.getLogger(
            'relay.TcpRelay({host_a}:{port_a} <-> {host_b}:{port_b})'
            .format(**self.__dict__))

    def __del__(self):
        self._logger.info('Closing sockets')
        self.socket_a.close()
        self.socket_b.close()

    def run(self):
        global _global_kill

        self._logger.info('Starting connection accept loop...')
        while True:
            try:
                if _global_kill.isSet():
                    self._logger.info('Received global kill')
                    break

                readable, writable, exceptable = select.select(
                    self._sockets, [], [], self.timeout)

                disconnect = False
                for sock in readable:
                    disconnect = disconnect or self._handleReadableSocket(sock)

                if disconnect:
                    self._logger.info(
                        'One of the connections closed, closing entire relay')
                    break

            except Exception as e:
                self._logger.error(
                    'Caught exception while waiting for "recv": {:}'
                    .format(e))
                break

        self._logger.info('Terminating')

    def _handleReadableSocket(self, src_socket):
        msg = src_socket.recv(self.max_message_size)
        self._logger.debug('Message received: {!r}'.format(msg))

        if src_socket is self.socket_a:
            src_name, dst_name = 'A', 'B'
            src_host, dst_host = self.host_a, self.host_b
            src_port, dst_port = self.port_a, self.port_b
            dst_socket = self.socket_b
        elif src_socket is self.socket_b:
            src_name, dst_name = 'B', 'A'
            src_host, dst_host = self.host_b, self.host_a
            src_port, dst_port = self.port_b, self.port_a
            dst_socket = self.socket_a
        else:
            self._logger.warning(
                'Message received from unknown socket! How is '
                'this possible?! Ignoring.')
            return False

        if not msg:
            self._logger.info(
                'Connection to host {:} closed'.format(src_name))
            return True

        self._routeMessage( msg, src_name, src_host, src_port,
                                 dst_name, dst_host, dst_port )
        return False

    def _routeMessage( self, msg, src_name, src_host, src_port,
                                  dst_name, dst_host, dst_port ):
        self._logger.debug(
            'Message received from host {:} ({!s}:{!s}), routing to host {:} '
            '({!s}:{!s})'
            .format(
                src_name, src_host, src_port,
                dst_name, dst_host, dst_port))

        dst_socket.send(msg)


class TcpRelayServer(Thread):

    def __init__(
            self,
            host_a, host_b, port,
            max_message_size=16384,
            timeout=1.0,
            backlog=5,
            relay=TcpRelay):
        super(TcpRelayServer, self).__init__()

        self.host_a = host_a
        self.host_b = host_b
        self.port = port
        self.max_message_size = max_message_size
        self.timeout = timeout
        self.backlog = backlog
        self.relay = relay

        self._logger = logging.getLogger(
            'relay.TcpRelayServer({host_a} <-> {host_b}, {port})'
            .format(**self.__dict__))

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.socket.settimeout(self.timeout)
        self._logger.info(
            'Successfully set socket timeout to {timeout}'
            .format(**self.__dict__))

        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._logger.info('Successfully set socket option to SO_REUSEADDR')

        self._logger.info(
            'Trying to bind socket to ("", {port})'
            .format(**self.__dict__))
        self.socket.bind(('', self.port))
        self._logger.info(
            'Successfully bound socket to ("", {port})'
            .format(**self.__dict__))

        self._logger.info(
            'Trying to start listenining to socket'
            .format(**self.__dict__))
        self.socket.listen(self.backlog)
        self._logger.info(
            'Successfully started listening to socket (backlog={backlog})'
            .format(**self.__dict__))

    def __del__(self):
        self._logger.info('Closing socket')
        self.socket.close()

    def run(self):
        global _global_kill

        self._logger.info('Starting connection accept loop...')
        while True:
            try:
                if _global_kill.isSet():
                    self._logger.info('Received global kill')
                    break

                src_socket, (src_host, src_port) = self.socket.accept()
                self._handleConnection(src_socket, src_host, src_port)

            except socket.timeout:
                pass

            except Exception as e:
                self._logger.error(
                    'Caught exception while waiting for "accept": {:}'
                    .format(e))
                break

        self._logger.info('Terminating')

    def _handleConnection(self, src_socket, src_host, src_port):
        if _global_kill.isSet():
            self._logger.info(
                'Received new connection request but received global kill so '
                'closing it')
            src_socket.close()
            return

        self._logger.info(
            'Accepted connection from ({:}, {:})'
            .format(src_host, src_port))
        if src_host == self.host_a:
            src_name, dst_name = 'A', 'B'
            dst_host = self.host_b
        elif src_host == self.host_b:
            src_name, dst_name = 'B', 'A'
            dst_host = self.host_a
        else:
            self._logger.warn(
                'Accepted connection is from unknown host, ignoring and '
                'closing connection')
            src_socket.close()
            return

        self._logger.info(
            'Accepted connection is from host {:} ({!s}), setting up forward '
            'connection to host {:} ({!s}, {!s})'
            .format(src_name, src_host, dst_name, dst_host, self.port))

        try:
            dst_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            dst_socket.connect((dst_host, self.port))
        except Exception as e:
            self._logger.error(
                'Caught exception while setting up forwarding connection to '
                '({:}, {:}): {:}'
                .format(dst_host, self.port, e))
            self._logger.info(
                'Closing new connection from ({:}, {:})'
                .format(src_host, src_port))
            src_socket.close()
            return

        self._logger.info(
            'Connection to ({!s}, {!s}) successful'
            .format(dst_host, self.port))

        relay = self.relay(
            src_socket, (src_host, src_port),
            dst_socket, (dst_host, self.port),
            self.max_message_size)
        relay.start()


def main():
    logger = logging.getLogger('relay')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    parser = argparse.ArgumentParser(
        description='Acts as a TCP and UDP relay from one interface to '
                    'another. This means any message received on this machine '
                    'on one of the ports specified from one of the hosts is '
                    'relayed to the other host across the same protocol and '
                    'same port. Handy for, say, tunneling some networking out '
                    'of a VM to an external network.')
    parser.add_argument(
        'host_a',
        help='Host name or IP address for interface A.')
    parser.add_argument(
        'host_b',
        help='Host name or IP address for interface B.')
    parser.add_argument(
        '--udp', '-u',
        dest='udp_port',
        type=int,
        nargs='+',
        default=[],
        help='UDP port numbers to relay.')
    parser.add_argument(
        '--tcp', '-t',
        dest='tcp_port',
        type=int,
        nargs='+',
        default=[],
        help='TCP port numbers to relay.')
    parser.add_argument(
        '--max-message-size',
        '-m',
        dest='max_message_size',
        type=int,
        default=16384,
        help='Sets the max message size in bytes (default=16384)')
    parser.add_argument(
        '--tcp-connection-backlog',
        '-b',
        dest='backlog',
        type=int,
        default=5,
        help='Sets the backlog size for TCP connections (default=5)')

    def substitution( option ):
        """Accepts substitution patterns bracketed like sed, eg.

            -s/pattern/replacement/
            -s:pattern:replacement:

        using a character not contained in either pattern nor replacment to bracket 
        """
        assert option and option[0] == option[-1], \
            "substitution %r must be bound by the same character; in this case: %r" % (
                option, option[0] )
        patsub = tuple( option[1:-1].split( option[0] ))
        assert len( patsub ) == 2, \
            "substitution %r must contain exactly one %r within the bracketing %r's" % (
                option, option[0], option[0] )
        return patsub

    parser.add_argument(
        '--substitute',
        '-s',
        type=substitution,
        action='append',
        default=[],
        help='Substitute every match of first Regular Expression with the second value' )
        
    args = parser.parse_args()

    class RelaySubstitutions( object ):
        """Capture and perform any --sub /pat/sub/ substitutions, if specified.  Add to
        {Tcp,Udp}Relay class to perform any configured substitutions, in order, before messasge is
        relayed."""

        substitute = args.substitute # Closure over args.substitute, capturing it for later use

        def _routeMessage( self, msg, *args, **kwds ):
            for pat,sub in self.substitute:
                res = re.sub( pat, sub, msg )
                if res != msg:
                    self._logger.debug('Message transform {!r}'.format(res))
                msg = res
            super( RelaySubstitutions, self )._routeMessage( msg, *args, **kwds )

    udp_relay = UdpRelay
    tcp_relay = TcpRelay
    if args.substitute:
        for pat,sub in args.substitute:
            logger.info( "substitutions: {!r} --> {!r}".format( pat, sub ))

        class udp_relay( RelaySubstitutions, UdpRelay ):
            pass

        class tcp_relay( RelaySubstitutions, TcpRelay ):
            pass

    ip_a = socket.gethostbyname(args.host_a)
    logger.info(
        'Host name A "{:}" interpreted as IP "{:}"'.format(args.host_a, ip_a))
    ip_b = socket.gethostbyname(args.host_b)
    logger.info(
        'Host name B "{:}" interpreted as IP "{:}"'.format(args.host_b, ip_b))

    relays = []

    for udp_port in args.udp_port:
        relay = udp_relay(ip_a, ip_b, udp_port, args.max_message_size)
        relays.append(relay)

    for tcp_port in args.tcp_port:
        relay = TcpRelayServer(
            ip_a, ip_b, tcp_port, args.max_message_size, args.backlog, relay=tcp_relay)
        relays.append(relay)

    for relay in relays:
        relay.start()

    try:
        for relay in relays:
            while relay.is_alive():
                relay.join(timeout=10.0)
    except (KeyboardInterrupt, SystemExit):
        logger.info('Killing threads')
        _global_kill.set()


if __name__ == '__main__':
    main()
