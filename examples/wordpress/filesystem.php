<?php

require_once ABSPATH . 'wp-admin/includes/class-wp-filesystem-direct.php';

class WP_Filesystem_Parallelefs extends WP_Filesystem_Direct
{
  private const UPGRADE_PATH = WP_CONTENT_DIR . '/upgrade/';
  private const CORE_UPGRADE_PATTERN = '%^' . self::UPGRADE_PATH . '(?:wp_|wordpress-)[^/]+/wordpress/(.*)$%';
  private const PLUGIN_UPGRADE_PATTERN = '%^' . self::UPGRADE_PATH . '[^/]+/(.*)$%';
  private const THEME_UPGRADE_PATTERN = '%^' . self::UPGRADE_PATH . '[^/]+/(.*)$%';

  private $nested = false;
  private $socket = null;
  private $speculateCallback = null;

  private static function mktemp()
  {
    $temp = tempnam(sys_get_temp_dir(), '');
    unlink($temp);
    mkdir($temp);
    chmod($temp, 0700);
    return $temp;
  }

  private static function socket_exists($file, $timeout = 2.0)
  {
    $start_at = microtime(true);

    do {
      if (file_exists($file)) {
        return true;
      }
      usleep(100 * 1000);
    } while (microtime(true) - $start_at < $timeout);

    return false;
  }

  private static function backtrace()
  {
    $frames = [];
    foreach (debug_backtrace() as $frame) {
      if (!array_key_exists('file', $frame)) {
        continue;
      }
      $frames[] = '  "' . $frame['file'] . ':' . $frame['line'] . "\",\n";
    }
    return "[\n" . implode('', $frames) . ']';
  }

  private static function error($message)
  {
    error_log("[WP_Filesystem_Parallelefs error]$message");
    error_log(self::backtrace());
  }

  private static function warn($message)
  {
    error_log("[WP_Filesystem_Parallelefs warn]$message");
    error_log(self::backtrace());
  }

  private static function debug($message)
  {
    if (defined('PARALLELEFS_DEBUG') && PARALLELEFS_DEBUG) {
      error_log("[WP_Filesystem_Parallelefs debug]$message");
    }
  }

  public function __construct($arg)
  {
    // This class pretends to be "direct" method for compatibility.
    parent::__construct($arg);

    $temp = self::mktemp();
    $socket_file = "$temp/parallelefs.sock";

    $bin = defined('PARALLELEFS_PATH') ? PARALLELEFS_PATH : '/usr/local/bin/parallelefs';
    $log = defined('PARALLELEFS_LOG') ? PARALLELEFS_LOG : '/var/log/parallelefs.log';

    $command = "$bin -s $socket_file >> $log 2>&1 & echo $!";
    $server_pid = exec($command);

    register_shutdown_function(function () use ($server_pid) {
      if (!$this->finalize()) {
        self::error('failed to finalize connection to parallelefs');
      }
      exec("kill $server_pid");
    });

    $socket = socket_create(AF_UNIX, SOCK_STREAM, 0);
    if ($socket === false) {
      self::error('failed to create socket for WP_Filesystem_Parallelefs');
      return;
    }

    if (!self::socket_exists($socket_file)) {
      self::error('failed to find a socket file created by parallelefs');
      return;
    }

    if (!socket_connect($socket, $socket_file)) {
      self::error('failed to connect to parallelefs');
      return;
    }

    $this->socket = $socket;
  }

  private static function encode_parallelefs_data($value)
  {
    return json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . "\n";
  }

  private function run_on_parallelefs($data)
  {
    if (socket_write($this->socket, $data) === false) {
      self::error('failed to send data to parallelefs');
      return false;
    }

    $res = socket_read($this->socket, 1024 * 1024, PHP_NORMAL_READ);
    if ($res === false) {
      self::error(
        'failed to receive data from parallelefs: code: ' . socket_last_error()
      );
      return false;
    }

    $resv = json_decode($res);

    if ($resv === null) {
      self::error('invalid input to parallelefs: ' . $data);
      return false;
    }

    return $resv;
  }

  public function finalize()
  {
    return $this->run_on_parallelefs("\n");
  }

  private function call_with_decorator($decorator, $func, ...$args)
  {
    if ($this->nested) {
      return call_user_func($func, ...$args);
    }

    try {
      $this->nested = true;

      return $decorator(function () use ($func, $args) {
        return call_user_func($func, ...$args);
      });
    } finally {
      $this->nested = false;
    }
  }

  private static function log_trace($t_start, $t_end, $caller)
  {
    $dt = DateTime::createFromFormat('U.u', sprintf('%.6f', $t_start));
    $timestamp = $dt->format('Y-m-d H:i:s.v');

    self::debug(sprintf(
      '[%23s] %-20.19s %02.6fs',
      $timestamp,
      $caller,
      $t_end - $t_start
    ));
  }

  private function trace_parent($method, ...$args)
  {
    $caller = debug_backtrace()[1]['function'];
    return $this->call_with_decorator(function ($func) use ($caller) {
      $t = microtime(true);
      try {
        return call_user_func($func);
      } finally {
        self::log_trace($t, microtime(true), $caller);
      }
    }, ['parent', $method], ...$args);
  }

  private function trace_func($func)
  {
    $caller = debug_backtrace()[1]['function'];
    return $this->call_with_decorator(function ($func) use ($caller) {
      $t = microtime(true);
      try {
        return call_user_func($func);
      } finally {
        self::log_trace($t, microtime(true), $caller);
      }
    }, $func);
  }

  private function call_parent($method, ...$args)
  {
    return $this->call_with_decorator(function ($func) {
      return call_user_func($func);
    }, ['parent', $method], ...$args);
  }

  public function abspath()
  {
    return $this->call_parent('abspath');
  }

  public function wp_content_dir()
  {
    return $this->call_parent('wp_content_dir');
  }

  public function wp_plugins_dir()
  {
    return $this->call_parent('wp_plugins_dir');
  }

  public function wp_themes_dir($theme = false)
  {
    return $this->call_parent('wp_themes_dir', $theme);
  }

  public function wp_lang_dir()
  {
    return $this->call_parent('wp_lang_dir');
  }

  public function find_base_dir($base = '.', $echo = false)
  {
    return $this->call_parent('find_base_dir', $base, $echo);
  }

  public function get_base_dir($base = '.', $echo = false)
  {
    return $this->call_parent('get_base_dir', $base, $echo);
  }

  public function find_folder($folder)
  {
    return $this->call_parent('find_folder', $folder);
  }

  public function search_for_folder($folder, $base = '.', $loop = false)
  {
    return $this->call_parent('search_for_folder', $folder, $base, $loop);
  }

  public function gethchmod($file)
  {
    // The existence of $file must be checked in advance.
    return $this->trace_parent('gethchmod', $file);
  }

  public function getchmod($file)
  {
    // The existence of $file must be checked in advance.
    return $this->trace_parent('getchmod', $file);
  }

  public function getnumchmodfromh($mode)
  {
    return $this->call_parent('getnumchmodfromh', $mode);
  }

  public function is_binary($text)
  {
    return $this->call_parent('is_binary', $text);
  }

  public function chown($file, $owner, $recursive = false)
  {
    // The existence of $file must be checked in advance.
    if ($recursive) {
      // TODO: Delegate this to parallelefs for faster chown.
      self::warn("chown (recursive) called: file: $file, owner: $owner");
    }
    return $this->trace_parent('chown', $file, $owner, $recursive);
  }

  private function updateSpeculationCallback()
  {
    $trace = debug_backtrace();

    // Determine what files to speculate based on caller class.
    for ($i = count($trace) - 1; $i >= 0; $i--) {
      $frame = $trace[$i];

      if (array_key_exists('file', $frame)) {
        if (strstr($frame['file'], '/wp-content/plugins/') !== false) {
          $this->speculateCallback = null;
          return;
        }
        if (strstr($frame['file'], '/wp-content/themes/') !== false) {
          $this->speculateCallback = null;
          return;
        }
      }

      if (array_key_exists('class', $frame)) {
        $name = $frame['class'];
      } else if (array_key_exists('function', $frame)) {
        $name = $frame['function'];
      } else {
        continue;
      }

      switch ($name) {
        case 'Core_Upgrader':
        case 'do_core_upgrade':
          $this->speculateCallback = function ($path) {
            if (preg_match(self::CORE_UPGRADE_PATTERN, $path, $matches)) {
              return ABSPATH . $matches[1];
            }
            return null;
          };
          return;
        case 'Language_Pack_Upgrader':
          // TODO: Support language packs.
          $this->speculateCallback = null;
          return;
        case 'Plugin_Upgrader':
          $this->speculateCallback = function ($path) {
            if (preg_match(self::PLUGIN_UPGRADE_PATTERN, $path, $matches)) {
              return WP_PLUGIN_DIR . '/' . $matches[1];
            }
            return null;
          };
          return;
        case 'Theme_Upgrader':
          $this->speculateCallback = function ($path) {
            if (preg_match(self::THEME_UPGRADE_PATTERN, $path, $matches)) {
              return get_theme_root() . '/' . $matches[1];
            }
            return null;
          };
          return;
        case 'wp_ajax_delete_plugin':
        case 'wp_ajax_delete_theme':
        case 'delete_plugins':
        case 'delete_theme':
        case 'wp_can_install_language_pack':
        case 'WP_Site_Health_Auto_Updates':
        case '_wp_delete_all_temp_backups':
          $this->speculateCallback = null;
          return;
      }
    }

    self::warn('could not find out how to speculate');
    $this->speculateCallback = null;
  }

  public function connect()
  {
    $this->updateSpeculationCallback();
    return $this->call_parent('connect');
  }

  public function get_contents($file)
  {
    if ($this->speculateCallback) {
      // The existence of $file must be checked in advance.
      self::warn("get_contents called: file: $file");
    }
    return $this->trace_parent('get_contents', $file);
  }

  public function get_contents_array($file)
  {
    if ($this->speculateCallback) {
      // The existence of $file must be checked in advance.
      self::warn("get_contents_array called: file: $file");
    }
    return $this->trace_parent('get_contents_array', $file);
  }

  public function put_contents($file, $contents, $mode = false)
  {
    return $this->trace_func(function () use ($file, $contents, $mode) {
      if ($this->speculateCallback) {
        $speculate_path = call_user_func($this->speculateCallback, $file);
        if ($speculate_path) {
          $this->run_on_parallelefs(self::encode_parallelefs_data([
            'dest' => $speculate_path,
            'speculate' => true,
            'perm' => FS_CHMOD_FILE,
          ]));
        }
      }

      if (strpos($file, self::UPGRADE_PATH) === 0) {
        // `upgrade` dir is assumed to be on fast EBS volume.
        return parent::put_contents($file, $contents, $mode);
      }

      $req = [
        'dest' => $file,
        'content_b64' => base64_encode($contents),
      ];

      if ($mode) {
        $req['perm'] = $mode;
      }

      return $this->run_on_parallelefs(self::encode_parallelefs_data($req));
    });
  }

  public function cwd()
  {
    if ($this->speculateCallback) {
      // This won't be called.
      self::warn("cwd called");
    }
    return $this->call_parent('cwd');
  }

  public function chdir($dir)
  {
    if ($this->speculateCallback) {
      // This won't be called.
      self::warn("chdir called: dir: $dir");
    }
    return $this->call_parent('chdir', $dir);
  }

  public function chgrp($file, $group, $recursive = false)
  {
    if ($this->speculateCallback) {
      // This won't be called.
      self::warn("chgrp called: file: $file");
    }
    return $this->call_parent('chgrp', $file, $group, $recursive);
  }

  public function chmod($file, $mode = false, $recursive = false)
  {
    // The existence of $file must be checked in advance.
    if ($recursive) {
      // TODO: Delegate this to parallelefs for faster chmod.
      self::warn("chmod (recursive) called: file: $file, mode: $mode");
    }
    return $this->trace_parent('chmod', $file, $mode, $recursive);
  }

  public function owner($file)
  {
    return $this->call_parent('owner', $file);
  }

  public function group($file)
  {
    return $this->trace_parent('group', $file);
  }

  public function copy($source, $destination, $overwrite = false, $mode = false)
  {
    return $this->trace_func(function () use ($source, $destination, $overwrite, $mode) {
      if (strpos($destination, self::UPGRADE_PATH) === 0) {
        return parent::copy($source, $destination, $overwrite, $mode);
      }

      if (!$overwrite && $this->exists($destination)) {
        return false;
      }

      $req = [
        'dest' => $destination,
        'src' => $source,
      ];

      if ($mode) {
        $req['perm'] = $mode;
      }

      return $this->run_on_parallelefs(self::encode_parallelefs_data($req));
    });
  }

  public function move($source, $destination, $overwrite = false)
  {
    if ($this->speculateCallback) {
      // Use core's fallback mechanism.
      return false;
    }
    return $this->trace_parent('move', $source, $destination, $overwrite);
  }

  public function delete($file, $recursive = false, $type = false)
  {
    return $this->trace_func(function () use ($file, $recursive, $type) {
      if (strpos($file, self::UPGRADE_PATH) === 0) {
        // `upgrade` dir is assumed to be on fast EBS volume.
        return parent::delete($file);
      }

      if ($type == 'f' || $this->is_file($file) || (!$recursive && $this->is_dir($file))) {
        return $this->run_on_parallelefs(self::encode_parallelefs_data([
          'dest' => $file,
          'delete' => true,
        ]));
      }

      return $this->run_on_parallelefs(self::encode_parallelefs_data([
        'dest' => $file,
        'delete_recursive' => true,
      ]));
    });
  }

  public function exists($file)
  {
    return $this->trace_func(function () use ($file) {
      if (strpos($file, self::UPGRADE_PATH) === 0) {
        // `upgrade` dir is assumed to be on fast EBS volume.
        return parent::exists($file);
      }

      return $this->run_on_parallelefs(self::encode_parallelefs_data([
        'dest' => $file,
        'existence' => true,
      ]));
    });
  }

  public function is_file($file)
  {
    return $this->trace_func(function () use ($file) {
      if (strpos($file, self::UPGRADE_PATH) === 0) {
        // `upgrade` dir is assumed to be on fast EBS volume.
        return parent::is_file($file);
      }

      $exists = $this->run_on_parallelefs(self::encode_parallelefs_data([
        'dest' => $file,
        'existence' => true,
      ]));

      if (!$exists) {
        return false;
      }

      return parent::is_file($file);
    });
  }

  public function is_dir($path)
  {
    return $this->trace_func(function () use ($path) {
      if (strpos($path, self::UPGRADE_PATH) === 0) {
        // `upgrade` dir is assumed to be on fast EBS volume.
        return parent::is_dir($path);
      }

      $exists = $this->run_on_parallelefs(self::encode_parallelefs_data([
        'dest' => $path,
        'existence' => true,
      ]));

      if (!$exists) {
        return false;
      }

      return parent::is_dir($path);
    });
  }

  public function is_readable($file)
  {
    return $this->trace_parent('is_readable', $file);
  }

  public function is_writable($file)
  {
    // The existence of $file must be checked in advance.
    return $this->trace_parent('is_writable', $file);
  }

  public function atime($file)
  {
    if ($this->speculateCallback) {
      // This won't be called.
      self::warn("atime called: file: $file");
    }
    return $this->trace_parent('atime', $file);
  }

  public function mtime($file)
  {
    // The existence of $file must be checked in advance.
    return $this->trace_parent('mtime', $file);
  }

  public function size($file)
  {
    // The existence of $file must be checked in advance.
    return $this->trace_parent('size', $file);
  }

  public function touch($file, $time = 0, $atime = 0)
  {
    if ($this->speculateCallback) {
      // This won't be called.
      self::warn("touch called: file: $file");
    }
    return $this->trace_parent('touch', $file, $time, $atime);
  }

  public function mkdir($path, $chmod = false, $chown = false, $chgrp = false)
  {
    return $this->trace_func(function () use ($path, $chmod, $chown, $chgrp) {
      if (strpos($path, self::UPGRADE_PATH) === 0) {
        // `upgrade` dir is assumed to be on fast EBS volume.
        return parent::mkdir($path, $chmod, $chown, $chgrp);
      }

      $req = [
        'dest' => $path,
        'mkdir' => true,
      ];

      if ($chmod) {
        $req['perm'] = $chmod;
      }

      $succeeded = $this->run_on_parallelefs(self::encode_parallelefs_data($req));

      if (!$succeeded) {
        return false;
      }

      if ($chown) {
        $this->chown($path, $chown);
      }

      if ($chgrp) {
        $this->chgrp($path, $chgrp);
      }

      return true;
    });
  }

  public function rmdir($path, $recursive = false)
  {
    if ($this->speculateCallback) {
      // This won't be called.
      self::warn("rmdir called: path: $path");
    }
    return $this->delete($path, $recursive);
  }

  public function dirlist($path, $include_hidden = true, $recursive = false)
  {
    return $this->trace_func(function () use ($path, $include_hidden, $recursive) {
      if (strpos($path, self::UPGRADE_PATH) === 0) {
        // `upgrade` dir is assumed to be on fast EBS volume.
        return parent::dirlist($path, $include_hidden, $recursive);
      }

      if ($this->is_file($path)) {
        $limit_file = basename($path);
        $path = dirname($path);
      } else {
        $limit_file = false;
      }

      if (!$this->is_dir($path)) {
        return false;
      }

      $dir = $this->run_on_parallelefs(self::encode_parallelefs_data([
        'dest' => $path,
        'listdir' => true,
      ]));

      if (!$dir) {
        return false;
      }

      $ret = array();

      foreach ($dir as $entry) {
        $struc = array();
        $struc['name'] = $entry;

        if ('.' == $struc['name'] || '..' == $struc['name']) {
          continue;
        }

        if (!$include_hidden && '.' == $struc['name'][0]) {
          continue;
        }

        if ($limit_file && $struc['name'] != $limit_file) {
          continue;
        }

        $struc['perms']       = $this->gethchmod($path . '/' . $entry);
        $struc['permsn']      = $this->getnumchmodfromh($struc['perms']);
        $struc['number']      = false;
        $struc['owner']       = $this->owner($path . '/' . $entry);
        $struc['group']       = $this->group($path . '/' . $entry);
        $struc['size']        = $this->size($path . '/' . $entry);
        $struc['lastmodunix'] = $this->mtime($path . '/' . $entry);
        $struc['lastmod']     = date('M j', $struc['lastmodunix']);
        $struc['time']        = date('h:i:s', $struc['lastmodunix']);
        $struc['type']        = $this->is_dir($path . '/' . $entry) ? 'd' : 'f';

        if ('d' == $struc['type']) {
          if ($recursive) {
            $struc['files'] = $this->dirlist($path . '/' . $struc['name'], $include_hidden, $recursive);
          } else {
            $struc['files'] = array();
          }
        }

        $ret[$struc['name']] = $struc;
      }

      return $ret;
    });
  }
}
