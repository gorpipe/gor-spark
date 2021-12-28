FROM registry.gitlab.com/wuxi-nextcode/wxnc-gor/gor-spark:1.9.6

USER root
ENTRYPOINT ["/usr/bin/entrypoint.sh"]